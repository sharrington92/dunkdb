#' Create DunkDB Table: coaches dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_coaches <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS coaches (
      coach_id INT PRIMARY KEY,
      name TEXT
    )"
  )

}
