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
#' @param text Statement to print to console
#' @param status One of: start, done
#'
#' @returns N/A. Sends statement to console
#'
#' @examples
#' status_out("Test start", "start")
#' status_out("Test complete", "done")
status_out <- function(text, status, width = NULL, padded.char = "."){
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
