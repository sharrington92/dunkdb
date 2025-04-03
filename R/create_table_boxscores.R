#' Create DunkDB Table: boxscores fact
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_boxscores <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS box (
      game_id INT,
      player_id INT,
      tech INT,
      fouls INT,
      blocks INT,
      steals INT,
      turnover INT,
      assist INT,
      rebound_total INT,
      rebound_def INT,
      rebound_off INT,
      points INT,
      freethrow_pct DECIMAL(6, 3),

      FOREIGN KEY (game_id) REFERENCES games (game_id),
      FOREIGN KEY (player_id) REFERENCES players (player_id)
    )"
  )

}
