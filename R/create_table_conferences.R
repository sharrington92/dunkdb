#' Create DunkDB Table: conferences dimension
#'
#' @returns
#'
#' @examples
create_table_conferences <- function(con){

  DBI::dbExecute(
    con,
    "
    CREATE TABLE IF NOT EXISTS conferences (
      conf_id INT PRIMARY KEY,
      conf TEXT,
      conf_type TEXT,
      is_current BOOL
    )
    "
  )

}
