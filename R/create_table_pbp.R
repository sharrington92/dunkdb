#' Create DunkDB Table: play-by-play dimension
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_pbp <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE play_by_play (
    	game_id INT,
    	game_event_id INT,
    	period SMALLINT,
    	seconds_elapsed DECIMAL,
    	score_home INT,
    	score_away INT,
    	team_id INT,
    	player_id INT,
    	event_result NVARCHAR(25),
    	event_tags NVARCHAR(100),
    	event TEXT,

    	UNIQUE(game_id, game_event_id),
    	FOREIGN KEY (team_id) REFERENCES teams (team_id),
    	FOREIGN KEY (player_id) REFERENCES players (player_id)
    );"
  )

}
