#' Insert Data From Temp to Permanent Table
#'
#' @param con dunkdb database connection
#' @param temp_name Name of temp table
#' @param perm_name Name of permanent table
#' @param overwrite Default false. True if existing data should be removed.
#'
#' @returns N/A
#'
db_temp_to_perm <- function(con, temp_name, perm_name, overwrite = FALSE){

  if(overwrite) DBI::dbExecute(con, paste0("TRUNCATE TABLE ", perm_name))

  # Get column names in permanent table
  cols <- colnames(DBI::dbGetQuery(con, paste0("SELECT * FROM ", perm_name, " LIMIT 1")))

  DBI::dbExecute(
    con,
    paste0(
      "INSERT INTO ", perm_name,
      " SELECT ", paste(cols, collapse = ", ")," FROM '", temp_name, "'"
    )
  )
}


#' Print Status to Console
#'
#' @param text Statement to print to console.
#' @param status One of: start, done.
#' @param width Character width of output. NULL for no padding.
#' @param padded.char Character value used to get to specified width.
#'
#' @returns N/A. Sends statement to console
#'
#' @examples
#' status_out("Test start", "start")
#' status_out("Test complete", "done")
status_out <- function(text, status, width = 35, padded.char = "."){
  out_color <- switch(status,
                      "start" = "yellow2",
                      "done" = "green1")

  text_color <- crayon::style(text, out_color)
  text_length <- crayon::col_nchar(text_color)

  if(!is.null(width)) {
    padded_text <- paste0(text, paste0(rep(".", width - text_length), collapse = ""))
  } else {
    padded_text <- text
  }

  cat(crayon::style(padded_text, out_color))
}


#' Delete All Data from DunkDB
#'
#' @param con A database connection.
#' @param force When true, does not ask for confirmation.
#'
#' @returns N/A. Sends 'TRUNCATE TABLE...' query to database for all tables.
#'
truncate_db <- function(con, force = FALSE){

  response <- NULL

  if(!force){
    response <- readline(prompt = cat(
      " Are you sure you want to remove all data from DunkDB database?",
      "(yes/no): "
    ))
    response <- tolower(response)
  }

  if(force | response %in% c("y", "yes")){

    tbls <- dbListTables(con)

    qrys <- paste("TRUNCATE TABLE", tbls, collapse = ";\n", sep = " ")

    dbExecute(con, qrys)

  }
}


#' Parse a URL
#'
#' @param .url URL as string.
#'
#' @returns Parsed HTML object.
#'
#' @examples
#' parse_webpage("https://stats.ncaa.org/teams/history?utf8=%E2%9C%93&org_id=260&sport_code=MBB&commit=Search")
parse_webpage <- function(.url){

  # Form request
  req <- httr2::request(.url) |>
    httr2::req_headers(`User-Agent` = "My Custom User Agent")

  # Get response
  resp <- req |>
    httr2::req_perform() |>
    httr2::resp_body_string()

  # Parse html
  parsed.html <- resp |>
    rvest::read_html()

  return(parsed.html)
}
