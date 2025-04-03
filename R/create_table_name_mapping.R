#' Create DunkDB Table: name_mapping dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_name_mapping <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS name_mapping (
    	name TEXT,
    	nickname TEXT,
    	UNIQUE (name, nickname)
    )"
  )

}
