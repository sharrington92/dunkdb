#' Initialize DunkDB
#'
#' @description
#' *dunkdb*() creates a DuckDB database and the necessary database objects
#' to store NCAA men's basketball data in a normalized relational model.
#'
#' @param path File path for database location. If path = "" or
#' path = ":memory:", an in-memory database instance is created.
#' @param db_name Name of DuckDB database.
#'
#' @returns A DuckDB database connection.
#' @export
#'
#' @example
#' init_dunkdb()
init_dunkdb <- function(path = getwd(), db_name = "dunkdb.ddb"){

  if(path == "" | path == ':memory:') {
    db_full_path <- ':memory:'
  } else{
    db_full_path <- file.path(path, db_name)
  }

  # Create DuckDB Instance if needed
  ddb <- duckdb::duckdb(
    dbdir = db_full_path,
    read_only = FALSE
  )

  # Connect to DB Instance
  con <- DBI::dbConnect(ddb)


  # Load DuckDB Packages
  DBI::dbExecute(
    con,
    "
    INSTALL httpfs;
    "
  )


  # Create database tables
  create_tables(con)

  # Create database functions

  return(con)
}
