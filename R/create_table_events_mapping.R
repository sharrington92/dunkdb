#' Create DunkDB Table: events_mapping dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_events_mapping <- function(con){

  DBI::dbExecute(
    con,
    "CREATE OR REPLACE TABLE events_map AS
    WITH
    clean_events AS (
    	SELECT
    		event AS event_raw,
    		clean_staging_event(event) AS event,
    		COUNT() AS n,
    		COUNT(DISTINCT game_id) AS game_n
    	FROM staging_events
    	WHERE
    		event NOT LIKE '%,%'
    		AND event NOT LIKE '%;%'
    		AND event NOT LIKE '%  %'
    	GROUP BY ALL
    	HAVING game_n > 1
    ), event_sim AS (
    	SELECT
    		event_raw,
    		a.event AS event_clean,
    		b.event,
    		jaccard(a.event, b.event) AS similarity_jaccard,
    		damerau_levenshtein(a.event, b.event) AS similar_damer,
    		game_n
    	FROM clean_events a, events b
    	WINDOW
    		event_window AS (PARTITION BY event_raw ORDER BY similar_damer, similarity_jaccard DESC)
    	QUALIFY
    		row_number() OVER event_window <= 1
    )
    SELECT DISTINCT
    	event_raw,
    	event
    FROM
    	event_sim
    ORDER BY event, event_raw;"
  )

}
