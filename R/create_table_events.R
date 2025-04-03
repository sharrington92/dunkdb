#' Create DunkDB Table: events dimension
#'
#' @returns
#'
#' @examples
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
