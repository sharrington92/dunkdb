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

  populate_name_mapping(con)

}


#' Populate name_mapping table
#'
#' @description
#' Inserts a name-to-nickname [table](https://github.com/carltonnorthern/nicknames/raw/refs/heads/master/names.csv)
#' into database. This is for standardizing player names as play-by-play data
#' does not consistently use same first name.
#'
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
  	   );
    INSERT INTO name_mapping
    	VALUES
    		('corey', 'cory'),
    		('cory', 'corey')
    	ON CONFLICT DO NOTHING;
    "
    )
}
