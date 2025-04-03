#' Create DunkDB Table: seasons dimension
#'
#' @returns
#'
#' @examples
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
