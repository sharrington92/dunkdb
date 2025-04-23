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
  populate_schools(con)
  # populate_conferences(con, .seasons)
  # populate_teams(con, .seasons)
  # populate_coaches(con, .seasons, .teams)
  # populate_rosters(con, .seasons, .teams)
  # populate_players(con, .seasons, .teams)
  # populate_games(con, .seasons, .teams)
  # populate_boxscores(con, .seasons, .teams)
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
#' @description
#' This function collects data about schools from stats.ncaa.org and joins
#' it with various tables from the ncaahoopR package. These datasets are
#' normalized and used to populate the schools, school_alias, school_color,
#' and school_ref tables.
#'
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

  schools.scrape <-
    tibble(
      school_id = as.integer(school.ids)[-1],
      ncaa_name = schools[-1]
    )

  # Create list of tables with school names from ncaahoopR package
  school.tables <- list(
    schools.scrape,
    ncaa_colors |> dplyr::select(ncaa_name, espn_name),
    ncaahoopR::ids |> dplyr::select(team, espn_abbrv),
    ncaahoopR::dict |> dplyr::select(-conference)
  )

  # Iterate through each dataset creating inner and anti joins
  # with each column to get an accumulating match from the scraped names
  # with those in ncaahoopR package.
  joins <- list()
  index <- 1
  for(t in seq_along(school.tables)[-1]){
    df <- school.tables[[t]]
    for(i in seq_along(colnames(df))){
      if(index == 1){
        joins[[index]] <- school.tables[[1]] |>
          dplyr::select(school_id, ncaa_name) |>
          dplyr::inner_join(
            df,
            dplyr::join_by(ncaa_name == !!rlang::sym(colnames(df)[i]))
          )
      } else if(i == 1){
        joins[[index]] <- do.call(dplyr::bind_rows, joins) |>
          dplyr::select(school_id, ncaa_name) |>
          dplyr::inner_join(
            df,
            dplyr::join_by(ncaa_name == !!rlang::sym(colnames(df)[i]))
          )
      } else{
        joins[[index]] <-
          # joins[[index-1]] |>
          do.call(bind_rows, joins) |>
          dplyr::select(school_id, ncaa_name) |>
          dplyr::anti_join(df, join_by(ncaa_name == !!rlang::sym(colnames(df)[i-1]))) |>
          dplyr::inner_join(
            df,
            dplyr::join_by(ncaa_name == !!rlang::sym(colnames(df)[i]))
          )
      }
      index = index + 1
    }
  }

  # Bind joins together, and get distinct matches for each scraped name
  joins.df <- do.call(dplyr::bind_rows, purrr::discard(joins, ~nrow(.x) == 0)) |>
    dplyr::distinct() |>
    tidyr::pivot_longer(-c(school_id, ncaa_name), values_drop_na = TRUE) |>
    dplyr::distinct() |>
    tidyr::pivot_wider(names_from = name, values_from = value)


  # df_no_match <- schools.scrape |>
  #   dplyr::anti_join(
  #     y = joins.df,
  #     dplyr::join_by(ncaa_name)
  #   )

  df.together <- joins.df |>
    dplyr::left_join(
      y = ncaahoopR::ids |>
        dplyr::select(id, team, link),
      dplyr::join_by(espn_name == team)
    ) |>
    dplyr::left_join(
      y = ncaahoopR::ncaa_colors,
      dplyr::join_by(ncaa_name, espn_name)
    ) |>
    dplyr::rename(
      espn_id = id
    ) |>
    dplyr::rename_with(tolower)


  df.school <- df.together |>
    dplyr::select(school_id, espn_id, ncaa_name, espn_abbrv)

  df.school.alias_stage <- df.together |>
    dplyr::select(
      school_id, ncaa_name, espn_name, espn_pbp,
      warren_nolan, trank, name_247, sref_name
    ) |>
    tidyr::pivot_longer(
      -school_id,
      names_to = "source", values_to = "alias_name",
      values_drop_na = TRUE
    ) |>
    dplyr::mutate(source = stringr::str_replace(source, "_name", ""))

  df.alias.source <-
    df.school.alias_stage |>
    dplyr::distinct(source) |>
    dplyr::mutate(source_id = row_number()) |>
    dplyr::relocate(source_id, .before = source)

  df.school.alias <-
    df.school.alias_stage |>
    dplyr::left_join(
      y = df.alias.source,
      dplyr::join_by(source)
    ) |>
    dplyr::relocate(source_id, .before = source)

  df.school.color <-
    df.together |>
    dplyr::select(school_id, dplyr::contains("color")) |>
    dplyr::mutate(
      color_3 = dplyr::coalesce(tertiary_color, color_3),
      .keep = "unused"
    ) |>
    dplyr::rename(
      color_1 = primary_color,
      color_2 = secondary_color
    ) |>
    tidyr::pivot_longer(
      -school_id,
      names_to = "seq",
      values_to = "color_hex",
      names_prefix = "color_", names_transform = as.integer,
      values_drop_na = TRUE
    )

  df.school.ref <-
    df.together |>
    dplyr::select(school_id, link, logo_url, sref_link) |>
    dplyr::rename(espn_link = link) |>
    tidyr::pivot_longer(
      -school_id,
      names_to = "value_type",
      values_to = "value",
      values_drop_na = TRUE
    )


  df.list <- list(
    "schools" = df.school,
    "school_alias" = df.school.alias,
    "school_color" = df.school.color,
    "school_ref" = df.school.ref
  )

  dbExecute(con, paste("TRUNCATE TABLE", rev(names(df.list)), collapse = ";\n"))

  for(i in seq_along(df.list)){
    dbAppendTable(con, names(df.list)[i], df.list[[i]], overwrite = TRUE)
  }

  status_out("Complete", "done", width = NULL)
}
