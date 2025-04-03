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
