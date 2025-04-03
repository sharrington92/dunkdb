#' Create DunkDB Table: games fact
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_games <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS games (
      game_id INT PRIMARY KEY,
      box_id INT NOT NULL,
      start_dttm TIMESTAMP,
      home_team_id INT NOT NULL,
      away_team_id INT NOT NULL,
      home_score INT,
      away_score INT,
      attendance INT,
      neutral_site BOOL,

      FOREIGN KEY (home_team_id) REFERENCES teams (team_id),
      FOREIGN KEY (away_team_id) REFERENCES teams (team_id)
    )"
  )

}
