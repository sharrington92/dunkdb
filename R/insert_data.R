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

  # Get any school page
  u <- "https://stats.ncaa.org/teams/history?utf8=%E2%9C%93&org_id=260&sport_code=MBB&commit=Search"

  parsed.html <- parse_webpage(u)

  # Select the <select> element with id "year_list"
  school.select <- html_node(parsed.html, "select#org_id_select")

  # Extract all <option> nodes within the selected <select>
  option_nodes <- html_nodes(school.select, "option")

  # Extract the 'value' attributes
  school.ids <- html_attr(option_nodes, "value")

  # Extract the text labels and trim any leading/trailing whitespace
  schools <- html_text(option_nodes) %>% trimws()

  df.schools <-
    tibble(
      school_id = as.integer(school.ids)[-1],
      ncaa_name = schools[-1]
    ) #%>%
    # left_join(
    #   y = ncaahoopR::ncaa_colors %>%
    #     mutate(
    #       ncaa_name = case_when(
    #         ncaa_name == "Fairleigh Dickinson" ~ "FDU",
    #         ncaa_name == "IUPUI" ~ "IU Indy",
    #         ncaa_name == "Saint Francis (PA)" ~ "Saint Francis",
    #         is.character(ncaa_name) ~ ncaa_name
    #       )
    #     ) %>%
    #     left_join(
    #       y = ncaahoopR::ids %>%
    #         rename(
    #           espn_id = id,
    #           espn_link = link
    #         ),
    #       by = c("espn_name" = "team")
    #     ),
    #   by = "ncaa_name"
    # ) %>%
    # select(-c(color_3, conference))

  # Create list of tables with school names
  school.tables <- list(
    df.schools,
    ncaa_colors |> select(ncaa_name, espn_name),
    ncaahoopR::ids |> select(team, espn_abbrv),
    ncaahoopR::dict |> select(-conference)
  )

  # Iterate through each dataset creating inner and anti joins
  # with each column to get a accumulating match from the scraped names
  # with those in ncaahoopR package.
  joins <- list()
  index <- 1
  for(t in seq_along(school.tables)[-1]){
    df <- school.tables[[t]]
    for(i in seq_along(colnames(df))){
      if(index == 1){
        joins[[index]] <- school.tables[[1]] |>
          select(school_id, ncaa_name) |>
          inner_join(
            df,
            join_by(ncaa_name == !!sym(colnames(df)[i]))
          )
      } else if(i == 1){
        joins[[index]] <- do.call(bind_rows, joins) |>
          select(school_id, ncaa_name) |>
          inner_join(
            df,
            join_by(ncaa_name == !!sym(colnames(df)[i]))
          )
      } else{
        joins[[index]] <-
          # joins[[index-1]] |>
          do.call(bind_rows, joins) |>
          select(school_id, ncaa_name) |>
          anti_join(df, join_by(ncaa_name == !!sym(colnames(df)[i-1]))) |>
          inner_join(
            df,
            join_by(ncaa_name == !!sym(colnames(df)[i]))
          )
      }
      index = index + 1
    }
  }

  # Bind joins together, and get distinct matches for each scraped name
  joins.df <- do.call(bind_rows, purrr::discard(joins, ~nrow(.x) == 0)) |>
    distinct() |>
    tidyr::pivot_longer(-c(school_id, ncaa_name), values_drop_na = TRUE) |>
    distinct() |>
    tidyr::pivot_wider(names_from = name, values_from = value)

  # ncaahoopR::ids # espn ids
  # ncaahoopR::conf # conferences



  df.schools %>%
    dbAppendTable(duck.con, "schools", .)

  status_out("Complete", "done", width = NULL)
}
