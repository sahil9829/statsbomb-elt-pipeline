import pytest
from pyspark.sql import Row
from pyspark.sql import functions as F

import statsbomb_transform as job


@pytest.fixture()
def tables(spark, raw_lake):
    ingested_at = F.lit("2024-06-01 00:00:00").cast("timestamp")
    built = job.build_tables(spark, f"{raw_lake}/{job.RAW_PREFIX}", ingested_at)
    return {name: df.cache() for name, df in built.items()}


def rows_by(df, key):
    return {r[key]: r for r in df.collect()}


# --------------------------------------------------------------------------- #
# Contract: the job output must match what Terraform registers in Glue
# --------------------------------------------------------------------------- #
@pytest.mark.parametrize("table", ["competitions", "matches", "events", "lineups"])
def test_output_schema_matches_contract(tables, contract, table):
    actual = [(f.name, f.dataType.simpleString()) for f in tables[table].schema.fields]
    expected = [(c["name"], c["type"]) for c in contract[table]["columns"]]
    assert actual == expected


def test_contract_covers_every_table(contract):
    assert set(contract) == set(job.PRIMARY_KEYS)


# --------------------------------------------------------------------------- #
# Table-level behaviour
# --------------------------------------------------------------------------- #
def test_competitions_trims_names_and_parses_timestamps(tables):
    row = tables["competitions"].first()
    assert row.competition_name == "FIFA World Cup"
    assert row.match_updated_at.year == 2024


def test_matches_flattens_nested_structs(tables):
    m = tables["matches"].first()
    assert (m.match_id, m.home_team_name, m.away_team_name) == (1001, "France", "Croatia")
    assert m.home_manager_name == "France Manager"
    assert m.competition_stage_name == "Final"
    assert m.stadium_country_name == "Russia"
    assert m.referee_id is None  # null struct is tolerated
    assert str(m.match_date) == "2018-07-15"


def test_matches_deduplicates_keeping_latest(spark):
    raw = spark.createDataFrame(
        [Row(match_id=1, last_updated="2024-01-01T00:00:00", home_score=0),
         Row(match_id=1, last_updated="2024-02-01T00:00:00", home_score=2)]
    )
    # widen to the full raw schema so the transform can select every field
    full = spark.createDataFrame([], job.MATCHES_SCHEMA).unionByName(raw, allowMissingColumns=True)
    out = job.transform_matches(full, F.current_timestamp()).collect()
    assert len(out) == 1 and out[0].home_score == 2


def test_events_take_match_id_from_file_name(tables):
    assert {r.match_id for r in tables["events"].select("match_id").distinct().collect()} == {1001}


def test_events_flatten_pass_and_shot(tables):
    ev = rows_by(tables["events"], "event_id")
    p, goal, miss = ev["e2"], ev["e3"], ev["e4"]

    assert (p.location_x, p.location_y) == (60.0, 40.0)
    assert (p.pass_end_location_x, p.pass_end_location_y) == (90.0, 42.0)
    assert p.pass_recipient_id == 11 and p.pass_is_goal_assist and not p.pass_is_cross
    assert p.pass_outcome_name is None  # null outcome == completed pass in StatsBomb

    assert goal.shot_outcome_name == "Goal" and goal.shot_statsbomb_xg == pytest.approx(0.45)
    assert goal.shot_end_location_z == pytest.approx(1.2)
    assert goal.under_pressure is True and goal.shot_key_pass_id == "e2"

    # two-element end_location must not raise; z is simply null
    assert miss.shot_end_location_z is None
    assert miss.under_pressure is False  # absent flag defaults to False


def test_events_without_player_or_location(tables):
    starting_xi = rows_by(tables["events"], "event_id")["e1"]
    assert starting_xi.player_id is None and starting_xi.location_x is None
    assert starting_xi.tactics_formation == 442


def test_lineups_minutes_starters_and_cards(tables):
    lu = rows_by(tables["lineups"], "player_id")

    subbed_off = lu[10]
    assert subbed_off.is_starter and subbed_off.appeared
    assert (subbed_off.on_minute, subbed_off.off_minute) == (0.0, 75.5)

    full_match = lu[11]
    assert full_match.off_minute is None and full_match.off_reason == "Final Whistle"
    assert full_match.first_position_name == "Right Center Forward"
    assert (full_match.yellow_cards, full_match.red_cards) == (1, 0)

    bench = lu[12]
    assert not bench.appeared and not bench.is_starter and bench.on_minute is None

    sent_off_sub = lu[20]
    assert not sent_off_sub.is_starter
    assert (sent_off_sub.on_minute, sent_off_sub.off_minute) == (75.5, 88.25)
    assert (sent_off_sub.yellow_cards, sent_off_sub.red_cards) == (2, 1)  # second yellow counts as both


# --------------------------------------------------------------------------- #
# Data-quality gates
# --------------------------------------------------------------------------- #
def test_quality_gate_passes_clean_data(tables):
    for name, df in tables.items():
        assert job.check_quality(name, df) > 0


def test_quality_gate_rejects_duplicates(spark):
    df = spark.createDataFrame([("a",), ("a",)], "event_id string")
    with pytest.raises(job.DataQualityError, match="duplicate"):
        job.check_quality("events", df)


def test_quality_gate_rejects_null_keys(spark):
    df = spark.createDataFrame([(1, None, 3)], "match_id bigint, team_id bigint, player_id bigint")
    with pytest.raises(job.DataQualityError, match="null primary key"):
        job.check_quality("lineups", df)


def test_quality_gate_rejects_empty(spark):
    with pytest.raises(job.DataQualityError, match="no rows"):
        job.check_quality("matches", spark.createDataFrame([], "match_id bigint"))


# --------------------------------------------------------------------------- #
# End to end
# --------------------------------------------------------------------------- #
def test_run_writes_all_processed_tables(spark, raw_lake):
    counts = job.run(spark, str(raw_lake))
    assert counts == {"competitions": 1, "matches": 1, "events": 4, "lineups": 4}
    for name in counts:
        assert spark.read.parquet(f"{raw_lake}/{job.PROCESSED_PREFIX}/{name}").count() == counts[name]


def test_parse_args_ignores_glue_arguments():
    args = job.parse_args(["--JOB_NAME", "x", "--lake_root", "s3://bucket", "--job-bookmark-option", "disable"])
    assert args.lake_root == "s3://bucket"
