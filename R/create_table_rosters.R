#' Create DunkDB Table: rosters dimension
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_rosters <- function(con){

  DBI::dbExecute(
    con,
    "CREATE SEQUENCE IF NOT EXISTS roster_id_seq START 1;
    CREATE TABLE IF NOT EXISTS rosters (
    	roster_id INT DEFAULT nextval('roster_id_seq') PRIMARY KEY,
    	team_id INT,
    	player_id INT,
    	games_play INT,
    	games_start INT,
    	jersey INT,
    	class TEXT,
    	position TEXT,
    	height_in DECIMAL(6, 3)

    --	UNIQUE(roster_id),
    	--PRIMARY KEY (team_id, player_id),
    --	UNIQUE(team_id, player_id),
    --	FOREIGN KEY (player_id) REFERENCES players (player_id),
    --	FOREIGN KEY (team_id) REFERENCES teams (team_id)
    );"
  )

}
