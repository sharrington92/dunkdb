#' Create DunkDB Table: players dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_players <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS players (
      player_id INT PRIMARY KEY,
      name TEXT,
      name_last TEXT,
      name_first TEXT,
      suffix TEXT,
      hometown TEXT,
      high_school TEXT
    )"
  )

}
