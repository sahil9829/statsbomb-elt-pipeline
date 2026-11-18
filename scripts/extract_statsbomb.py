"""Extract StatsBomb open data into the raw zone of the data lake.

Pulls competitions, matches, events and lineups for the competition/season
pairs listed in ``STATSBOMB_COMPETITIONS`` and lands the untouched JSON in the
raw zone, mirroring the upstream layout::

    <LAKE_ROOT>/raw/statsbomb/competitions/competitions.json
    <LAKE_ROOT>/raw/statsbomb/matches/<competition_id>/<season_id>.json
    <LAKE_ROOT>/raw/statsbomb/events/<match_id>.json
    <LAKE_ROOT>/raw/statsbomb/lineups/<match_id>.json

``LAKE_ROOT`` is either an S3 URI (``s3://my-bucket``) or a local directory,
so the whole pipeline can run on a laptop or in CI without AWS.

Environment variables
---------------------
LAKE_ROOT               s3://bucket or a local path. Falls back to
                        s3://$S3_BUCKET_NAME, then ./data_lake.
STATSBOMB_COMPETITIONS  Comma separated ``competition_id:season_id`` pairs
                        (default ``43:3`` - FIFA World Cup 2018).
STATSBOMB_MAX_MATCHES   Optional cap on matches per season (handy for dev/CI).
STATSBOMB_FORCE         ``true`` re-downloads events/lineups already in the lake.
"""

import argparse
import json
import logging
import os
import sys
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from urllib.parse import urlparse

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

BASE_URL = "https://raw.githubusercontent.com/statsbomb/open-data/master/data"
RAW_PREFIX = "raw/statsbomb"

logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
log = logging.getLogger("extract_statsbomb")


# --------------------------------------------------------------------------- #
# Storage
# --------------------------------------------------------------------------- #
class LakeWriter:
    """Writes objects to either S3 or the local filesystem."""

    def __init__(self, lake_root: str):
        self.lake_root = lake_root.rstrip("/")
        parsed = urlparse(self.lake_root)
        self.is_s3 = parsed.scheme in ("s3", "s3a")
        if self.is_s3:
            import boto3  # imported lazily so local runs don't need AWS libs configured

            self.bucket = parsed.netloc
            self.base_key = parsed.path.lstrip("/")
            self.s3 = boto3.client("s3")
        else:
            self.base_path = Path(self.lake_root)

    def _key(self, relative: str) -> str:
        return f"{self.base_key}/{relative}".lstrip("/") if self.base_key else relative

    def exists(self, relative: str) -> bool:
        if self.is_s3:
            from botocore.exceptions import ClientError

            try:
                self.s3.head_object(Bucket=self.bucket, Key=self._key(relative))
                return True
            except ClientError as exc:
                if exc.response["Error"]["Code"] in ("404", "NoSuchKey", "NotFound"):
                    return False
                raise
        return (self.base_path / relative).exists()

    def write_json(self, relative: str, payload) -> str:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        if self.is_s3:
            key = self._key(relative)
            self.s3.put_object(Bucket=self.bucket, Key=key, Body=body, ContentType="application/json")
            return f"s3://{self.bucket}/{key}"
        target = self.base_path / relative
        target.parent.mkdir(parents=True, exist_ok=True)
        tmp = target.with_suffix(target.suffix + ".tmp")
        tmp.write_bytes(body)
        tmp.replace(target)  # atomic: readers never see a half-written file
        return str(target)


# --------------------------------------------------------------------------- #
# Source API
# --------------------------------------------------------------------------- #
def build_session() -> requests.Session:
    retry = Retry(
        total=5,
        backoff_factor=1.0,
        status_forcelist=(429, 500, 502, 503, 504),
        allowed_methods=("GET",),
    )
    session = requests.Session()
    session.mount("https://", HTTPAdapter(max_retries=retry, pool_maxsize=16))
    return session


def fetch(session: requests.Session, path: str):
    response = session.get(f"{BASE_URL}/{path}", timeout=60)
    response.raise_for_status()
    return response.json()


def parse_competitions(spec: str):
    pairs = []
    for item in filter(None, (s.strip() for s in spec.split(","))):
        try:
            competition_id, season_id = (int(x) for x in item.split(":"))
        except ValueError as exc:
            raise ValueError(f"Invalid competition spec '{item}', expected 'competition_id:season_id'") from exc
        pairs.append((competition_id, season_id))
    if not pairs:
        raise ValueError("STATSBOMB_COMPETITIONS is empty")
    return pairs


def resolve_lake_root(cli_value):
    if cli_value:
        return cli_value
    if os.getenv("LAKE_ROOT"):
        return os.environ["LAKE_ROOT"]
    if os.getenv("S3_BUCKET_NAME"):
        return f"s3://{os.environ['S3_BUCKET_NAME']}"
    return "./data_lake"


# --------------------------------------------------------------------------- #
# Extraction
# --------------------------------------------------------------------------- #
def extract_match_files(session, lake, match_id: int, force: bool) -> int:
    written = 0
    for entity in ("events", "lineups"):
        relative = f"{RAW_PREFIX}/{entity}/{match_id}.json"
        if not force and lake.exists(relative):
            continue
        lake.write_json(relative, fetch(session, f"{entity}/{match_id}.json"))
        written += 1
    return written


def run(lake_root: str, competitions, max_matches=None, force=False, workers=8) -> dict:
    session = build_session()
    lake = LakeWriter(lake_root)
    log.info("Landing raw StatsBomb data in %s", lake_root)

    all_competitions = fetch(session, "competitions.json")
    lake.write_json(f"{RAW_PREFIX}/competitions/competitions.json", all_competitions)
    log.info("Fetched %d competition-seasons", len(all_competitions))

    available = {(c["competition_id"], c["season_id"]) for c in all_competitions}
    match_ids = []
    for competition_id, season_id in competitions:
        if (competition_id, season_id) not in available:
            raise ValueError(f"Competition {competition_id}:{season_id} is not in the StatsBomb open data catalogue")
        matches = fetch(session, f"matches/{competition_id}/{season_id}.json")
        matches.sort(key=lambda m: (m["match_date"], m["match_id"]))
        if max_matches:
            matches = matches[:max_matches]
        lake.write_json(f"{RAW_PREFIX}/matches/{competition_id}/{season_id}.json", matches)
        match_ids.extend(m["match_id"] for m in matches)
        log.info("Competition %s season %s: %d matches", competition_id, season_id, len(matches))

    written, failures = 0, []
    with ThreadPoolExecutor(max_workers=workers) as pool:
        futures = {pool.submit(extract_match_files, session, lake, mid, force): mid for mid in match_ids}
        for future in as_completed(futures):
            try:
                written += future.result()
            except Exception as exc:  # collect and fail loudly at the end
                failures.append(futures[future])
                log.error("Match %s failed: %s", futures[future], exc)

    summary = {"matches": len(match_ids), "files_written": written, "failed_matches": failures}
    log.info("Extraction summary: %s", summary)
    if failures:
        raise RuntimeError(f"{len(failures)} matches failed to extract: {failures}")
    return summary


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--lake-root", help="s3://bucket[/prefix] or local directory")
    parser.add_argument("--competitions", default=os.getenv("STATSBOMB_COMPETITIONS", "43:3"))
    parser.add_argument("--max-matches", type=int, default=int(os.getenv("STATSBOMB_MAX_MATCHES", "0")) or None)
    parser.add_argument("--force", action="store_true", default=os.getenv("STATSBOMB_FORCE", "").lower() == "true")
    parser.add_argument("--workers", type=int, default=8)
    args = parser.parse_args(argv)

    try:
        run(
            lake_root=resolve_lake_root(args.lake_root),
            competitions=parse_competitions(args.competitions),
            max_matches=args.max_matches,
            force=args.force,
            workers=args.workers,
        )
    except Exception:
        log.exception("Extraction failed")
        return 1  # non-zero exit so Airflow marks the task as failed
    return 0


if __name__ == "__main__":
    sys.exit(main())
