"""PySpark job: raw StatsBomb JSON -> typed, flattened Parquet (processed zone).

Reads the raw zone written by ``scripts/extract_statsbomb.py`` and produces four
tables whose schemas are pinned by ``contracts/processed_tables.json`` (the same
contract Terraform uses to register the Glue Catalog tables):

* competitions - one row per competition-season
* matches      - one row per match
* events       - one row per event (type-specific structs flattened)
* lineups      - one row per player per team per match

The job is a full refresh: the raw zone holds the complete history, so each
run rebuilds the processed zone deterministically.

It runs unchanged in three places:

* locally / CI:   ``spark-submit spark/jobs/statsbomb_transform.py --lake-root ./data_lake``
* Airflow (local Spark against S3): ``--lake-root s3a://bucket`` (needs hadoop-aws)
* AWS Glue 5.0:   Terraform passes ``--lake-root s3://bucket`` as a default argument
"""

import argparse
import logging
import sys
from datetime import datetime, timezone
from functools import reduce

from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

log = logging.getLogger("statsbomb_transform")

RAW_PREFIX = "raw/statsbomb"
PROCESSED_PREFIX = "processed/statsbomb"

# --------------------------------------------------------------------------- #
# Source schemas. Explicit schemas (rather than inference) make the job fast,
# deterministic, and immune to a sample of files missing an optional field.
# --------------------------------------------------------------------------- #
ID_NAME = "STRUCT<id: BIGINT, name: STRING>"

COMPETITIONS_SCHEMA = """
    competition_id BIGINT, season_id BIGINT, country_name STRING, competition_name STRING,
    competition_gender STRING, competition_youth BOOLEAN, competition_international BOOLEAN,
    season_name STRING, match_updated STRING, match_available STRING
"""

_MANAGER = f"ARRAY<STRUCT<id: BIGINT, name: STRING, nickname: STRING, dob: STRING, country: {ID_NAME}>>"
MATCHES_SCHEMA = f"""
    match_id BIGINT, match_date STRING, kick_off STRING,
    competition STRUCT<competition_id: BIGINT, country_name: STRING, competition_name: STRING>,
    season STRUCT<season_id: BIGINT, season_name: STRING>,
    home_team STRUCT<home_team_id: BIGINT, home_team_name: STRING, home_team_gender: STRING,
                     country: {ID_NAME}, managers: {_MANAGER}>,
    away_team STRUCT<away_team_id: BIGINT, away_team_name: STRING, away_team_gender: STRING,
                     country: {ID_NAME}, managers: {_MANAGER}>,
    home_score INT, away_score INT, match_status STRING, last_updated STRING,
    metadata STRUCT<data_version: STRING>, match_week INT,
    competition_stage {ID_NAME},
    stadium STRUCT<id: BIGINT, name: STRING, country: {ID_NAME}>,
    referee STRUCT<id: BIGINT, name: STRING, country: {ID_NAME}>
"""

EVENTS_SCHEMA = f"""
    id STRING, `index` INT, period INT, `timestamp` STRING, `minute` INT, `second` INT,
    `type` {ID_NAME}, possession INT, possession_team {ID_NAME}, play_pattern {ID_NAME},
    team {ID_NAME}, player {ID_NAME}, position {ID_NAME},
    location ARRAY<DOUBLE>, duration DOUBLE, under_pressure BOOLEAN, counterpress BOOLEAN,
    `pass` STRUCT<recipient: {ID_NAME}, length: DOUBLE, angle: DOUBLE, height: {ID_NAME},
                  end_location: ARRAY<DOUBLE>, `type`: {ID_NAME}, body_part: {ID_NAME},
                  outcome: {ID_NAME}, cross: BOOLEAN, shot_assist: BOOLEAN, goal_assist: BOOLEAN>,
    carry STRUCT<end_location: ARRAY<DOUBLE>>,
    shot STRUCT<statsbomb_xg: DOUBLE, end_location: ARRAY<DOUBLE>, key_pass_id: STRING,
                technique: {ID_NAME}, `type`: {ID_NAME}, outcome: {ID_NAME}, body_part: {ID_NAME},
                first_time: BOOLEAN>,
    dribble STRUCT<outcome: {ID_NAME}>,
    duel STRUCT<`type`: {ID_NAME}, outcome: {ID_NAME}>,
    interception STRUCT<outcome: {ID_NAME}>,
    goalkeeper STRUCT<`type`: {ID_NAME}, outcome: {ID_NAME}>,
    foul_committed STRUCT<card: {ID_NAME}>,
    bad_behaviour STRUCT<card: {ID_NAME}>,
    substitution STRUCT<outcome: {ID_NAME}, replacement: {ID_NAME}>,
    tactics STRUCT<formation: INT>
"""

LINEUPS_SCHEMA = f"""
    team_id BIGINT, team_name STRING,
    lineup ARRAY<STRUCT<
        player_id: BIGINT, player_name: STRING, player_nickname: STRING, jersey_number: INT,
        country: {ID_NAME},
        cards: ARRAY<STRUCT<`time`: STRING, card_type: STRING, reason: STRING, period: INT>>,
        positions: ARRAY<STRUCT<position_id: BIGINT, position: STRING, `from`: STRING, `to`: STRING,
                                from_period: INT, to_period: INT, start_reason: STRING,
                                end_reason: STRING>>>>
"""


# --------------------------------------------------------------------------- #
# Helpers
# --------------------------------------------------------------------------- #
def read_json(spark: SparkSession, path: str, schema: str) -> DataFrame:
    """Read multi-line JSON arrays (StatsBomb files are one array per file)."""
    return (
        spark.read.schema(schema)
        .option("multiLine", True)
        .option("mode", "FAILFAST")  # a malformed file should fail the run, not silently drop rows
        .json(path)
    )


def match_id_from_path(path_col: Column) -> Column:
    """events/<match_id>.json and lineups/<match_id>.json carry the match id only in the file name."""
    return F.regexp_extract(path_col, r"(\d+)\.json$", 1).cast("bigint")


def array_item(arr: Column, idx: int) -> Column:
    """Null-safe positional access (avoids ANSI-mode index errors on short arrays)."""
    return F.when(F.size(arr) > idx, arr.getItem(idx))


def clock_to_minutes(clock: Column) -> Column:
    """'MM:SS' lineup clock -> decimal minutes."""
    parts = F.split(clock, ":")
    return F.round(parts.getItem(0).cast("double") + parts.getItem(1).cast("double") / 60.0, 2)


def to_ts(col: Column) -> Column:
    return F.to_timestamp(col)


# --------------------------------------------------------------------------- #
# Transformations (pure DataFrame -> DataFrame so they are unit-testable)
# --------------------------------------------------------------------------- #
def transform_competitions(raw: DataFrame, ingested_at: Column) -> DataFrame:
    return raw.select(
        "competition_id",
        "season_id",
        F.trim("competition_name").alias("competition_name"),
        "country_name",
        "competition_gender",
        "competition_youth",
        "competition_international",
        "season_name",
        to_ts("match_updated").alias("match_updated_at"),
        to_ts("match_available").alias("match_available_at"),
        ingested_at.alias("_ingested_at"),
    ).dropDuplicates(["competition_id", "season_id"])


def transform_matches(raw: DataFrame, ingested_at: Column) -> DataFrame:
    def team(side: str):
        t = F.col(f"{side}_team")
        return [
            t[f"{side}_team_id"].alias(f"{side}_team_id"),
            t[f"{side}_team_name"].alias(f"{side}_team_name"),
            t[f"{side}_team_gender"].alias(f"{side}_team_gender"),
            t["country"]["name"].alias(f"{side}_team_country_name"),
            array_item(t["managers"], 0)["name"].alias(f"{side}_manager_name"),
        ]

    return (
        raw.select(
            "match_id",
            F.to_date("match_date").alias("match_date"),
            "kick_off",
            F.col("competition.competition_id").alias("competition_id"),
            F.col("competition.competition_name").alias("competition_name"),
            F.col("season.season_id").alias("season_id"),
            F.col("season.season_name").alias("season_name"),
            *team("home"),
            *team("away"),
            "home_score",
            "away_score",
            "match_status",
            "match_week",
            F.col("competition_stage.id").alias("competition_stage_id"),
            F.col("competition_stage.name").alias("competition_stage_name"),
            F.col("stadium.id").alias("stadium_id"),
            F.col("stadium.name").alias("stadium_name"),
            F.col("stadium.country.name").alias("stadium_country_name"),
            F.col("referee.id").alias("referee_id"),
            F.col("referee.name").alias("referee_name"),
            F.col("referee.country.name").alias("referee_country_name"),
            F.col("metadata.data_version").alias("data_version"),
            to_ts("last_updated").alias("last_updated_at"),
            ingested_at.alias("_ingested_at"),
        )
        # a match can appear in more than one season file only if upstream re-publishes; keep latest
        .withColumn(
            "_rn",
            F.row_number().over(
                Window.partitionBy("match_id").orderBy(F.col("last_updated_at").desc_nulls_last())
            ),
        )
        .filter("_rn = 1")
        .drop("_rn")
    )


def transform_events(raw: DataFrame, ingested_at: Column) -> DataFrame:
    p, s = F.col("pass"), F.col("shot")
    return raw.select(
        F.col("id").alias("event_id"),
        match_id_from_path(F.col("_metadata.file_path")).alias("match_id"),
        F.col("index").alias("event_index"),
        "period",
        F.col("timestamp").alias("event_timestamp"),
        "minute",
        "second",
        F.col("type.id").alias("type_id"),
        F.col("type.name").alias("type_name"),
        "possession",
        F.col("possession_team.id").alias("possession_team_id"),
        F.col("play_pattern.id").alias("play_pattern_id"),
        F.col("play_pattern.name").alias("play_pattern_name"),
        F.col("team.id").alias("team_id"),
        F.col("team.name").alias("team_name"),
        F.col("player.id").alias("player_id"),
        F.col("player.name").alias("player_name"),
        F.col("position.id").alias("position_id"),
        F.col("position.name").alias("position_name"),
        array_item(F.col("location"), 0).alias("location_x"),
        array_item(F.col("location"), 1).alias("location_y"),
        "duration",
        F.coalesce("under_pressure", F.lit(False)).alias("under_pressure"),
        F.coalesce("counterpress", F.lit(False)).alias("counterpress"),
        p["recipient"]["id"].alias("pass_recipient_id"),
        p["length"].alias("pass_length"),
        p["angle"].alias("pass_angle"),
        p["height"]["name"].alias("pass_height_name"),
        array_item(p["end_location"], 0).alias("pass_end_location_x"),
        array_item(p["end_location"], 1).alias("pass_end_location_y"),
        p["type"]["name"].alias("pass_type_name"),
        p["body_part"]["name"].alias("pass_body_part_name"),
        p["outcome"]["name"].alias("pass_outcome_name"),
        F.coalesce(p["cross"], F.lit(False)).alias("pass_is_cross"),
        F.coalesce(p["shot_assist"], F.lit(False)).alias("pass_is_shot_assist"),
        F.coalesce(p["goal_assist"], F.lit(False)).alias("pass_is_goal_assist"),
        array_item(F.col("carry.end_location"), 0).alias("carry_end_location_x"),
        array_item(F.col("carry.end_location"), 1).alias("carry_end_location_y"),
        s["statsbomb_xg"].alias("shot_statsbomb_xg"),
        s["outcome"]["name"].alias("shot_outcome_name"),
        s["type"]["name"].alias("shot_type_name"),
        s["body_part"]["name"].alias("shot_body_part_name"),
        s["technique"]["name"].alias("shot_technique_name"),
        array_item(s["end_location"], 0).alias("shot_end_location_x"),
        array_item(s["end_location"], 1).alias("shot_end_location_y"),
        array_item(s["end_location"], 2).alias("shot_end_location_z"),
        s["key_pass_id"].alias("shot_key_pass_id"),
        F.coalesce(s["first_time"], F.lit(False)).alias("shot_first_time"),
        F.col("dribble.outcome.name").alias("dribble_outcome_name"),
        F.col("duel.type.name").alias("duel_type_name"),
        F.col("duel.outcome.name").alias("duel_outcome_name"),
        F.col("interception.outcome.name").alias("interception_outcome_name"),
        F.col("goalkeeper.type.name").alias("goalkeeper_type_name"),
        F.col("goalkeeper.outcome.name").alias("goalkeeper_outcome_name"),
        F.col("foul_committed.card.name").alias("foul_committed_card_name"),
        F.col("bad_behaviour.card.name").alias("bad_behaviour_card_name"),
        F.col("substitution.replacement.id").alias("substitution_replacement_id"),
        F.col("substitution.outcome.name").alias("substitution_outcome_name"),
        F.col("tactics.formation").alias("tactics_formation"),
        ingested_at.alias("_ingested_at"),
    ).dropDuplicates(["event_id"])


def transform_lineups(raw: DataFrame, ingested_at: Column) -> DataFrame:
    exploded = raw.select(
        match_id_from_path(F.col("_metadata.file_path")).alias("match_id"),
        "team_id",
        "team_name",
        F.explode("lineup").alias("p"),
    )
    positions = F.col("p.positions")
    first_pos = array_item(positions, 0)
    last_pos = F.when(F.size(positions) > 0, F.element_at(positions, -1))

    def count_cards(*card_types):
        return F.size(F.filter(F.coalesce(F.col("p.cards"), F.array()), lambda c: c["card_type"].isin(*card_types)))

    return exploded.select(
        "match_id",
        "team_id",
        "team_name",
        F.col("p.player_id").alias("player_id"),
        F.col("p.player_name").alias("player_name"),
        F.col("p.player_nickname").alias("player_nickname"),
        F.col("p.jersey_number").alias("jersey_number"),
        F.col("p.country.id").alias("country_id"),
        F.col("p.country.name").alias("country_name"),
        (F.coalesce(F.size(positions), F.lit(0)) > 0).alias("appeared"),
        F.coalesce(first_pos["start_reason"] == "Starting XI", F.lit(False)).alias("is_starter"),
        first_pos["position_id"].alias("first_position_id"),
        first_pos["position"].alias("first_position_name"),
        clock_to_minutes(first_pos["from"]).alias("on_minute"),
        first_pos["from_period"].alias("on_period"),
        # null 'to' => still on the pitch at the final whistle; dbt resolves it to match length
        clock_to_minutes(last_pos["to"]).alias("off_minute"),
        last_pos["to_period"].alias("off_period"),
        last_pos["end_reason"].alias("off_reason"),
        count_cards("Yellow Card", "Second Yellow").cast("int").alias("yellow_cards"),
        count_cards("Red Card", "Second Yellow").cast("int").alias("red_cards"),
        ingested_at.alias("_ingested_at"),
    ).dropDuplicates(["match_id", "team_id", "player_id"])


# --------------------------------------------------------------------------- #
# Data-quality gates: fail the job before bad data reaches the processed zone.
# --------------------------------------------------------------------------- #
PRIMARY_KEYS = {
    "competitions": ["competition_id", "season_id"],
    "matches": ["match_id"],
    "events": ["event_id"],
    "lineups": ["match_id", "team_id", "player_id"],
}


class DataQualityError(Exception):
    pass


def check_quality(name: str, df: DataFrame) -> int:
    keys = PRIMARY_KEYS[name]
    any_key_null = reduce(lambda a, b: a | b, [F.col(k).isNull() for k in keys])
    row = df.agg(
        F.count(F.lit(1)).alias("rows"),
        F.countDistinct(F.struct(*keys)).alias("distinct_keys"),
        F.sum(F.when(any_key_null, 1).otherwise(0)).alias("null_keys"),
    ).first()
    rows, distinct_keys, null_keys = row["rows"], row["distinct_keys"], row["null_keys"] or 0
    if rows == 0:
        raise DataQualityError(f"{name}: produced no rows")
    if null_keys:
        raise DataQualityError(f"{name}: {null_keys} rows with null primary key {keys}")
    if distinct_keys != rows:
        raise DataQualityError(f"{name}: {rows - distinct_keys} duplicate primary keys {keys}")
    log.info("%s: %d rows passed quality checks", name, rows)
    return rows


# --------------------------------------------------------------------------- #
# Orchestration
# --------------------------------------------------------------------------- #
def build_tables(spark: SparkSession, raw_root: str, ingested_at: Column) -> dict:
    return {
        "competitions": transform_competitions(
            read_json(spark, f"{raw_root}/competitions/*.json", COMPETITIONS_SCHEMA), ingested_at
        ),
        "matches": transform_matches(read_json(spark, f"{raw_root}/matches/*/*.json", MATCHES_SCHEMA), ingested_at),
        "events": transform_events(read_json(spark, f"{raw_root}/events/*.json", EVENTS_SCHEMA), ingested_at),
        "lineups": transform_lineups(read_json(spark, f"{raw_root}/lineups/*.json", LINEUPS_SCHEMA), ingested_at),
    }


def run(spark: SparkSession, lake_root: str) -> dict:
    lake_root = lake_root.rstrip("/")
    raw_root = f"{lake_root}/{RAW_PREFIX}"
    processed_root = f"{lake_root}/{PROCESSED_PREFIX}"
    ingested_at = F.lit(datetime.now(timezone.utc).replace(tzinfo=None)).cast("timestamp")

    counts = {}
    for name, df in build_tables(spark, raw_root, ingested_at).items():
        df = df.cache()
        counts[name] = check_quality(name, df)
        # small dimensions-ish tables -> single file; events keep Spark's parallelism
        out = df.coalesce(1) if name != "events" else df.repartition("match_id")
        out.write.mode("overwrite").option("compression", "snappy").parquet(f"{processed_root}/{name}")
        df.unpersist()
        log.info("Wrote %s -> %s/%s", name, processed_root, name)
    return counts


def build_spark(app_name: str = "statsbomb_transform") -> SparkSession:
    return (
        SparkSession.builder.appName(app_name)
        .config("spark.sql.session.timeZone", "UTC")
        # Athena (Hive tables) reads millisecond timestamps; Spark defaults to INT96/micros
        .config("spark.sql.parquet.outputTimestampType", "TIMESTAMP_MILLIS")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )


def parse_args(argv):
    parser = argparse.ArgumentParser(description="Transform raw StatsBomb JSON into processed Parquet")
    parser.add_argument("--lake-root", "--lake_root", dest="lake_root", required=True,
                        help="Data lake root: local path, s3a://bucket (local Spark) or s3://bucket (Glue)")
    # Glue injects its own arguments (--JOB_NAME, --job-bookmark-option, ...): ignore them.
    args, _unknown = parser.parse_known_args(argv)
    return args


def main(argv=None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
    args = parse_args(sys.argv[1:] if argv is None else argv)
    spark = build_spark()
    try:
        counts = run(spark, args.lake_root)
        log.info("Transform complete: %s", counts)
    finally:
        spark.stop()
    return 0


if __name__ == "__main__":
    sys.exit(main())
