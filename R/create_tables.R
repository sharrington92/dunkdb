#' Create DunkDB Database Tables
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_tables <- function(con){

  create_table_name_mapping(con)
  create_table_schools(con)
  create_table_seasons(con)
  create_table_conferences(con)
  create_table_teams(con)
  create_table_rosters(con)
  create_table_players(con)
  create_table_coaches(con)
  create_table_games(con)
  create_table_boxscores(con)
  create_table_event_tags(con)
  create_table_events(con)
  create_table_events_mapping(con)
  create_table_pbp(con)

}
