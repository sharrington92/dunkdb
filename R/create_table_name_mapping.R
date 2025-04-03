#' Create DunkDB Table: name_mapping dimension
#'
#' @returns
#'
#' @examples
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
