#' Create DunkDB Table: seasons dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_seasons <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS seasons (
      season_id INT PRIMARY KEY,
      season TEXT,
      academic_year INT
    )"
  )

}
