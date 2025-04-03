#' Create DunkDB Table: coaches dimension
#'
#' @returns
#'
#' @examples
create_table_coaches <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS coaches (
      coach_id INT PRIMARY KEY,
      name TEXT
    )"
  )

}
