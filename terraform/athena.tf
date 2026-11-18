resource "aws_athena_workgroup" "this" {
  name          = local.name_prefix
  description   = "StatsBomb analytics: dbt runs and ad-hoc queries."
  force_destroy = var.force_destroy_buckets

  configuration {
    enforce_workgroup_configuration    = true
    publish_cloudwatch_metrics_enabled = true
    bytes_scanned_cutoff_per_query     = var.athena_bytes_scanned_cutoff

    engine_version {
      selected_engine_version = "Athena engine version 3"
    }

    result_configuration {
      output_location = "s3://${local.results_bucket}/query-results/"
      encryption_configuration {
        encryption_option = "SSE_S3"
      }
    }
  }
}

# Saved queries that show off the star schema in the Athena console.
resource "aws_athena_named_query" "top_scorers" {
  name      = "top-scorers-by-competition"
  workgroup = aws_athena_workgroup.this.id
  database  = aws_glue_catalog_database.this["marts"].name
  query     = <<-SQL
    select
        c.competition_season_name,
        p.player_display_name,
        t.team_name,
        sum(f.goals)               as goals,
        round(sum(f.xg), 2)        as xg,
        sum(f.assists)             as assists,
        round(sum(f.minutes_played)) as minutes
    from fct_player_match f
    join dim_player      p on p.player_key = f.player_key
    join dim_team        t on t.team_key = f.team_key
    join dim_competition c on c.competition_key = f.competition_key
    group by 1, 2, 3
    order by goals desc, xg desc
    limit 25
  SQL
}

resource "aws_athena_named_query" "xg_overperformance" {
  name      = "team-xg-over-performance"
  workgroup = aws_athena_workgroup.this.id
  database  = aws_glue_catalog_database.this["marts"].name
  query     = <<-SQL
    with team_games as (
        select home_team_key as team_key, home_score as goals, home_xg as xg from fct_matches
        union all
        select away_team_key, away_score, away_xg from fct_matches
    )
    select t.team_name, count(*) as matches, sum(goals) as goals,
           round(sum(xg), 2) as xg, round(sum(goals) - sum(xg), 2) as goals_minus_xg
    from team_games g
    join dim_team t on t.team_key = g.team_key
    group by 1
    order by goals_minus_xg desc
  SQL
}
