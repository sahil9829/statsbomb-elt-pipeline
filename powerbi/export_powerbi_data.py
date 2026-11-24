"""Export the game-state star schema from DuckDB to small CSVs for Power BI.

Run after ``make pipeline`` (see ``make powerbi-export``). Power BI Desktop reads
these with Get Data -> Text/CSV, so nobody needs Spark, dbt or DuckDB on Windows.
"""
import argparse
from pathlib import Path

import duckdb

SCHEMA = "statsbomb_marts"

# Only what the dashboard uses: tidy names, no surrogate-key noise beyond the join keys.
EXPORTS = {
    "fct_team_game_state": f"select * exclude (team_game_state_key, match_date_key, competition_key)"
                           f" from {SCHEMA}.fct_team_game_state",
    "fct_shots": f"select * exclude (shot_key, match_date_key, competition_key)"
                 f" from {SCHEMA}.fct_shots",
    "dim_game_state": f"select * from {SCHEMA}.dim_game_state",
    "dim_match": f"select * from {SCHEMA}.dim_match",
    "dim_team": f"select team_key, team_name from {SCHEMA}.dim_team"
                f" where team_key in (select team_key from {SCHEMA}.fct_team_game_state)",
    "dim_player": f"select player_key, player_name, player_display_name, position_group"
                  f" from {SCHEMA}.dim_player"
                  f" where player_key in (select player_key from {SCHEMA}.fct_shots)",
}


def main() -> None:
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--duckdb", default=str(root.parent / "dbt" / "statsbomb.duckdb"))
    parser.add_argument("--out", default=str(root / "data"))
    args = parser.parse_args()

    out = Path(args.out)
    out.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(args.duckdb, read_only=True)
    for name, query in EXPORTS.items():
        path = out / f"{name}.csv"
        con.execute(f"copy ({query} order by all) to '{path.as_posix()}' (header, delimiter ',')")
        rows = con.execute(f"select count(*) from ({query})").fetchone()[0]
        print(f"{name:<22} {rows:>6} rows -> {path}")


if __name__ == "__main__":
    main()
