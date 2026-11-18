import json

import pytest

import extract_statsbomb as ex

CATALOGUE = [{"competition_id": 43, "season_id": 3}]
MATCHES = [
    {"match_id": 2, "match_date": "2018-06-15"},
    {"match_id": 1, "match_date": "2018-06-14"},
    {"match_id": 3, "match_date": "2018-06-16"},
]


@pytest.fixture()
def fake_api(monkeypatch):
    calls = []

    def fake_fetch(_session, path):
        calls.append(path)
        if path == "competitions.json":
            return CATALOGUE
        if path.startswith("matches/"):
            return list(MATCHES)
        if path.startswith(("events/", "lineups/")):
            return [{"path": path}]
        raise AssertionError(f"unexpected path {path}")

    monkeypatch.setattr(ex, "fetch", fake_fetch)
    return calls


def test_parse_competitions():
    assert ex.parse_competitions("43:3, 72:107") == [(43, 3), (72, 107)]
    with pytest.raises(ValueError):
        ex.parse_competitions("43")
    with pytest.raises(ValueError):
        ex.parse_competitions(" , ")


def test_resolve_lake_root(monkeypatch):
    monkeypatch.delenv("LAKE_ROOT", raising=False)
    monkeypatch.delenv("S3_BUCKET_NAME", raising=False)
    assert ex.resolve_lake_root(None) == "./data_lake"
    monkeypatch.setenv("S3_BUCKET_NAME", "my-bucket")
    assert ex.resolve_lake_root(None) == "s3://my-bucket"
    monkeypatch.setenv("LAKE_ROOT", "/data")
    assert ex.resolve_lake_root(None) == "/data"
    assert ex.resolve_lake_root("s3://cli") == "s3://cli"


def test_run_lands_raw_layout_locally(tmp_path, fake_api):
    summary = ex.run(str(tmp_path), [(43, 3)], max_matches=2, workers=2)

    raw = tmp_path / "raw" / "statsbomb"
    assert (raw / "competitions" / "competitions.json").exists()
    matches = json.loads((raw / "matches" / "43" / "3.json").read_text())
    assert [m["match_id"] for m in matches] == [1, 2]  # earliest matches kept when capped
    for entity in ("events", "lineups"):
        assert sorted(p.name for p in (raw / entity).iterdir()) == ["1.json", "2.json"]
    assert summary == {"matches": 2, "files_written": 4, "failed_matches": []}


def test_run_is_incremental_unless_forced(tmp_path, fake_api):
    ex.run(str(tmp_path), [(43, 3)], workers=1)
    fake_api.clear()
    assert ex.run(str(tmp_path), [(43, 3)], workers=1)["files_written"] == 0
    assert not any(p.startswith("events/") for p in fake_api)
    assert ex.run(str(tmp_path), [(43, 3)], force=True, workers=1)["files_written"] == 6


def test_unknown_competition_fails(tmp_path, fake_api):
    with pytest.raises(ValueError, match="not in the StatsBomb"):
        ex.run(str(tmp_path), [(1, 1)])


def test_main_returns_non_zero_on_failure(tmp_path, monkeypatch):
    def boom(*_args, **_kwargs):
        raise RuntimeError("network down")

    monkeypatch.setattr(ex, "fetch", boom)
    assert ex.main(["--lake-root", str(tmp_path), "--competitions", "43:3"]) == 1
