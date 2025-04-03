#' Initialize DunkDB
#'
#' @description
#' *dunkdb*() creates a DuckDB database and the necessary database objects
#' to store NCAA men's basketball data in a normalized relational model.
#'
#' @param path File path for database location.
#' @param db_name Name of DuckDB database.
#'
#' @returns A DuckDB database connection.
#' @export
#'
init_dunkdb <- function(path = getwd(), db_name = "dunkdb.ddb"){

  # Create DuckDB Instance if needed
  ddb <- duckdb::duckdb(
    dbdir = file.path(path, db_name),
    read_only = FALSE
  )

  # Connect to DB Instance
  con <- DBI::dbConnect(ddb)


  # Create database tables
  create_tables()

  # Create database functions

}
