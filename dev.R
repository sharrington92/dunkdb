library(devtools)

{
  library(DBI)
  library(duckdb)
  library(httr2)
  library(rvest)
  library(dplyr)
  library(stringr)
}




load_all()
check()
document()



usethis::use_r("utils")
use_package("crayon")


{
  con <- init_dunkdb()

  dbListTables(con)

  insert_data(con)


  {
    status_out("\nPopulating names_mapping...", "start", width = 35)
    Sys.sleep(.5)
    status_out(" Complete", "done")

    status_out("\nPopulating seasons...", "start", width = 35)
    Sys.sleep(.5)
    status_out(" Complete", "done")

  }

  crayon::col_nchar("\nPopulating names_mapping...")
  nchar("\nPopulating names_mapping...")
}

dbDisconnect(con)
rm(con)
file.remove("dunkdb.ddb")
