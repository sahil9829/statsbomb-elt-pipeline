import json
from pathlib import Path

import pytest
from pyspark.sql import SparkSession

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def spark():
    session = (
        SparkSession.builder.master("local[1]")
        .appName("statsbomb-transform-tests")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    session.sparkContext.setLogLevel("ERROR")
    yield session
    session.stop()


@pytest.fixture(scope="session")
def contract():
    return json.loads((REPO_ROOT / "contracts" / "processed_tables.json").read_text())["tables"]


# --------------------------------------------------------------------------- #
# Minimal but structurally faithful StatsBomb payloads
# --------------------------------------------------------------------------- #
COMPETITIONS = [
    {"competition_id": 43, "season_id": 3, "country_name": "International", "competition_name": "FIFA World Cup ",
     "competition_gender": "male", "competition_youth": False, "competition_international": True,
     "season_name": "2018", "match_updated": "2024-01-01T10:00:00.000", "match_available": "2024-01-01T10:00:00.000"},
]


def _team(side, team_id, name):
    return {f"{side}_team_id": team_id, f"{side}_team_name": name, f"{side}_team_gender": "male",
            f"{side}_team_group": None, "country": {"id": team_id, "name": name},
            "managers": [{"id": team_id * 10, "name": f"{name} Manager", "nickname": None, "dob": None,
                          "country": {"id": team_id, "name": name}}]}


MATCHES = [
    {"match_id": 1001, "match_date": "2018-07-15", "kick_off": "17:00:00.000",
     "competition": {"competition_id": 43, "country_name": "International", "competition_name": "FIFA World Cup"},
     "season": {"season_id": 3, "season_name": "2018"},
     "home_team": _team("home", 771, "France"), "away_team": _team("away", 785, "Croatia"),
     "home_score": 1, "away_score": 0, "match_status": "available", "last_updated": "2024-01-01T10:00:00",
     "metadata": {"data_version": "1.0.2"}, "match_week": 7, "competition_stage": {"id": 26, "name": "Final"},
     "stadium": {"id": 1, "name": "Luzhniki", "country": {"id": 188, "name": "Russia"}},
     "referee": None},
]

EVENTS_1001 = [
    {"id": "e1", "index": 1, "period": 1, "timestamp": "00:00:00.000", "minute": 0, "second": 0,
     "type": {"id": 35, "name": "Starting XI"}, "possession": 1, "possession_team": {"id": 771, "name": "France"},
     "play_pattern": {"id": 1, "name": "Regular Play"}, "team": {"id": 771, "name": "France"},
     "tactics": {"formation": 442, "lineup": []}},
    {"id": "e2", "index": 2, "period": 1, "timestamp": "00:10:00.000", "minute": 10, "second": 0,
     "type": {"id": 30, "name": "Pass"}, "possession": 2, "possession_team": {"id": 771, "name": "France"},
     "play_pattern": {"id": 1, "name": "Regular Play"}, "team": {"id": 771, "name": "France"},
     "player": {"id": 10, "name": "Player Ten"}, "position": {"id": 23, "name": "Center Forward"},
     "location": [60.0, 40.0], "duration": 1.2,
     "pass": {"recipient": {"id": 11, "name": "Player Eleven"}, "length": 30.0, "angle": 0.1,
              "height": {"id": 1, "name": "Ground Pass"}, "end_location": [90.0, 42.0],
              "body_part": {"id": 40, "name": "Right Foot"}, "goal_assist": True, "shot_assist": True}},
    {"id": "e3", "index": 3, "period": 1, "timestamp": "00:10:02.000", "minute": 10, "second": 2,
     "type": {"id": 16, "name": "Shot"}, "possession": 2, "possession_team": {"id": 771, "name": "France"},
     "play_pattern": {"id": 1, "name": "Regular Play"}, "team": {"id": 771, "name": "France"},
     "player": {"id": 11, "name": "Player Eleven"}, "position": {"id": 22, "name": "Right Center Forward"},
     "location": [108.0, 40.0], "under_pressure": True,
     "shot": {"statsbomb_xg": 0.45, "end_location": [120.0, 39.0, 1.2], "key_pass_id": "e2",
              "outcome": {"id": 97, "name": "Goal"}, "type": {"id": 87, "name": "Open Play"},
              "body_part": {"id": 40, "name": "Right Foot"}, "technique": {"id": 93, "name": "Normal"},
              "freeze_frame": [{"location": [110.0, 40.0], "teammate": False}]}},
    {"id": "e4", "index": 4, "period": 2, "timestamp": "00:01:00.000", "minute": 46, "second": 0,
     "type": {"id": 16, "name": "Shot"}, "possession": 3, "possession_team": {"id": 785, "name": "Croatia"},
     "play_pattern": {"id": 4, "name": "From Throw In"}, "team": {"id": 785, "name": "Croatia"},
     "player": {"id": 20, "name": "Player Twenty"}, "position": {"id": 23, "name": "Center Forward"},
     "location": [100.0, 30.0],
     "shot": {"statsbomb_xg": 0.05, "end_location": [118.0, 45.0],
              "outcome": {"id": 98, "name": "Off T"}, "type": {"id": 87, "name": "Open Play"},
              "body_part": {"id": 37, "name": "Head"}, "technique": {"id": 93, "name": "Normal"}}},
]

LINEUPS_1001 = [
    {"team_id": 771, "team_name": "France", "lineup": [
        {"player_id": 10, "player_name": "Player Ten", "player_nickname": "Ten", "jersey_number": 10,
         "country": {"id": 78, "name": "France"}, "cards": [],
         "positions": [{"position_id": 23, "position": "Center Forward", "from": "00:00", "to": "75:30",
                        "from_period": 1, "to_period": 2, "start_reason": "Starting XI",
                        "end_reason": "Substitution - Off (Tactical)"}]},
        {"player_id": 11, "player_name": "Player Eleven", "player_nickname": None, "jersey_number": 11,
         "country": {"id": 78, "name": "France"},
         "cards": [{"time": "80:00", "card_type": "Yellow Card", "reason": "Foul Committed", "period": 2}],
         "positions": [{"position_id": 22, "position": "Right Center Forward", "from": "00:00", "to": "60:00",
                        "from_period": 1, "to_period": 2, "start_reason": "Starting XI",
                        "end_reason": "Tactical Shift"},
                       {"position_id": 23, "position": "Center Forward", "from": "60:00", "to": None,
                        "from_period": 2, "to_period": None, "start_reason": "Tactical Shift",
                        "end_reason": "Final Whistle"}]},
        {"player_id": 12, "player_name": "Bench Player", "player_nickname": None, "jersey_number": 23,
         "country": {"id": 78, "name": "France"}, "cards": [], "positions": []},
    ]},
    {"team_id": 785, "team_name": "Croatia", "lineup": [
        {"player_id": 20, "player_name": "Player Twenty", "player_nickname": None, "jersey_number": 9,
         "country": {"id": 56, "name": "Croatia"},
         "cards": [{"time": "50:00", "card_type": "Yellow Card", "reason": "Foul Committed", "period": 2},
                   {"time": "70:00", "card_type": "Second Yellow", "reason": "Foul Committed", "period": 2}],
         "positions": [{"position_id": 23, "position": "Center Forward", "from": "75:30", "to": "88:15",
                        "from_period": 2, "to_period": 2, "start_reason": "Substitution - On (Tactical)",
                        "end_reason": "Second Yellow"}]},
    ]},
]


def write_raw_lake(root: Path):
    raw = root / "raw" / "statsbomb"
    files = {
        raw / "competitions" / "competitions.json": COMPETITIONS,
        raw / "matches" / "43" / "3.json": MATCHES,
        raw / "events" / "1001.json": EVENTS_1001,
        raw / "lineups" / "1001.json": LINEUPS_1001,
    }
    for path, payload in files.items():
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(json.dumps(payload, indent=2))  # multi-line, like the real files
    return root


@pytest.fixture()
def raw_lake(tmp_path):
    return write_raw_lake(tmp_path / "lake")
