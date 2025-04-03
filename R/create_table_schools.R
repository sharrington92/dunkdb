#' Create DunkDB Table: schools dimension
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_schools <- function(con){

  DBI::dbExecute(
    con,
    "
    CREATE TABLE IF NOT EXISTS schools (
      school_id INT PRIMARY KEY,
      espn_id INT,
      ncaa_name TEXT,
      espn_name TEXT,
      espn_abbrv TEXT,
      primary_color TEXT,
      secondary_color TEXT,
      tertiary_color TEXT,
      color_4 TEXT,
      color_5 TEXT,
      color_6 TEXT,
      logo_url TEXT,
  	espn_link TEXT
    )
    "
  )

}
