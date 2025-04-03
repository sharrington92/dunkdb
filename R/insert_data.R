#' Insert Data Into DunkDB
#'
#' @description
#' Gets data from stats.ncaa.org and inserts into DunkDB database.
#'
#'
#' @param con A database connection to DunkDB database.
#'
#' @returns N/A. Inserts data but doesn't return anything.
#' @export
#'
insert_data <- function(con){

}


#' Populate name_mapping table
#'
#' @param con A database connection to DunkDB database.
#'
#' @returns N/A
#'
populate_name_mapping <- function(con){
  DBI::dbExecute(
    con,
    "
    TRUNCATE TABLE name_mapping;
    INSERT INTO TABLE name_mapping
  	SELECT
  		REGEXP_EXTRACT(name_row, '^([A-z]+),(.*)$', 1) AS name,
  		REGEXP_SPLIT_TO_TABLE(name_row, ',') AS nickname
  	FROM READ_CSV(
  		'https://github.com/carltonnorthern/nicknames/raw/refs/heads/master/names.csv',
  		header = FALSE,
  		names = ['name_row']
  	   );"
    )
}
