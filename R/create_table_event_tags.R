#' Create DunkDB Table: event_tags dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_event_tags <- function(con){

  DBI::dbExecute(
    con,
    "CREATE OR REPLACE TABLE event_tags AS
      WITH step1 AS (
      	SELECT
      		STRUCT_EXTRACT(tag_struct, 'unnest') AS tag
      	FROM staging_events
      	CROSS JOIN UNNEST(split(event_tags, ';')) AS tag_struct
      	WHERE
      		event_tags IS NOT NULL
      		AND TRIM(event_tags) != ''
      )
      SELECT DISTINCT
      	tag
      FROM step1
      WHERE
      	tag != ''
      ORDER BY tag;"
  )

}
