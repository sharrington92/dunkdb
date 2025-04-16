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
  populate_seasons(con)
  populate_schools(con, .seasons)
  populate_conferences(con, .seasons)
  populate_teams(con, .seasons)
  populate_coaches(con, .seasons, .teams)
  populate_rosters(con, .seasons, .teams)
  populate_players(con, .seasons, .teams)
  populate_games(con, .seasons, .teams)
  populate_boxscores(con, .seasons, .teams)
  # populate_events(con)
  # populate_events_mapping(con)
  # populate_event_tags(con)
  # populate_play_by_play(con, .games)

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

  status_out("\n\nPopulating names_mapping...", "start")

  DBI::dbExecute(
    con,
    "
    LOAD httpfs;
    TRUNCATE TABLE name_mapping;
    INSERT INTO name_mapping
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

  status_out("Complete", "done", width = NULL)
}


#' Insert Data for seasons Dimensional Table
#'
#' @param con Dunkdb database connection
#'
#' @returns N/A
#'
populate_seasons <- function(con){

  status_out("\nPopulating seasons...", "start")

  # Get any team page
  u <- "https://stats.ncaa.org/teams/560624"

  # Form request
  req <- httr2::request(u) |>
    httr2::req_headers(`User-Agent` = "My Custom User Agent")

  # Get response
  resp <- req |>
    httr2::req_perform() |>
    httr2::resp_body_string()

  # Parse html
  parsed.html <- rvest::read_html(resp)

  # Select the <select> element with id "year_list"
  year_select <- rvest::html_node(parsed.html, "select#year_list")

  # Extract all <option> nodes within the selected <select>
  option_nodes <- rvest::html_nodes(year_select, "option")

  # Extract the 'value' attributes
  season.ids <- rvest::html_attr(option_nodes, "value")

  # Extract the text labels and trim any leading/trailing whitespace
  seasons <- rvest::html_text(option_nodes) |> trimws()

  season <- NULL

  d <- dplyr::tibble(
    season_id = as.integer(season.ids),
    season = seasons,
    academic_year = as.numeric(stringr::str_sub(season, 1, 4)) + 1,
    ncaa_id = TRUE
  )

  DBI::dbWriteTable(con, "#seasons", d, temporary = TRUE, overwrite = TRUE)

  db_temp_to_perm(con, "#seasons", "seasons", overwrite = TRUE)


  # Since seasons is only available from 1972,
  # prior years will need to be generated with the ncaa_id flag set to false.
  season_id <- NULL
  dplyr::tibble(
    season_id = 1890:1970,
    season = paste0(
      season_id, "-",
      formatC((season_id + 1) %% 100, width = 2, flag = "0")
    ),
    academic_year = season_id,
    ncaa_id = FALSE
  ) %>%
    DBI::dbAppendTable(con, "seasons", .)

  status_out("Complete", "done", width = NULL)
}


#' Insert Data for seasons Dimensional Table
#'
#' @param con Dunkdb database connection
#'
#' @returns N/A
#'
populate_schools <- function(con){

  status_out("\nPopulating schools...", "start")



  status_out("Complete", "done", width = NULL)
}
