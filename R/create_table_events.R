#' Create DunkDB Table: events dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_events <- function(con){

  DBI::dbExecute(
    con,
    "CREATE SEQUENCE IF NOT EXISTS event_id_seq START 1;
    CREATE TABLE IF NOT EXISTS events (
    	event_id INT DEFAULT nextval('event_id_seq') PRIMARY KEY,
    	event TEXT,
    	event_type TEXT,
    	event_desc TEXT,

    	UNIQUE(event)
    );"
  )

}
