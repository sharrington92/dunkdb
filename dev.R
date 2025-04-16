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


}


{
  library(ncaahoopR)

  # Not up-to-date
  ncaahoopR::conf %>% as_tibble() #%>% filter(str_detect(sref_name, "Gonzaga"))

  # Seems up-to-date
  ncaahoopR::dict %>% as_tibble() #%>% filter(str_detect(NCAA, "Utah Tech"))

  # ncaahoopR::wp_hoops %>% as_tibble()

  ncaahoopR::ids %>% as_tibble()

  ncaahoopR::ncaa_colors %>% as_tibble()

  {
    df.schools |>
      inner_join(y = ncaa_colors, join_by(ncaa_name)) |>
      select(school_id, ncaa_name, espn_name)

    df.schools |>
      anti_join(y = ncaa_colors, join_by(ncaa_name)) |>
      select(school_id, ncaa_name) |>
      # 0 matches
      # inner_join(y = ncaa_colors, join_by(ncaa_name == espn_name))
      # 0 matches
      # inner_join(y = ncaahoopR::ids, join_by(ncaa_name == team))
      # 1 match
      inner_join(y = ncaahoopR::ids, join_by(ncaa_name == espn_abbrv)) |>
      select(school_id, ncaa_name)

    df.schools |>
      anti_join(y = ncaa_colors, join_by(ncaa_name)) |>
      select(school_id, ncaa_name) |>
      # 0 matches
      # inner_join(y = ncaa_colors, join_by(ncaa_name == espn_name))
      # 0 matches
      # inner_join(y = ncaahoopR::ids, join_by(ncaa_name == team))
      # 1 match
      anti_join(y = ncaahoopR::ids, join_by(ncaa_name == espn_abbrv)) |>
      select(school_id, ncaa_name) |>
      inner_join(y = ncaahoopR::dict, join_by(ncaa_name == Trank))

    colnames(ncaahoopR::dict)

    df.schools |>
      inner_join(y = ncaa_colors, join_by(ncaa_name == espn_name))


    df.schools |>
      inner_join(y = ncaahoopR::dict, join_by(ncaa_name == NCAA))
    df.schools |>
      inner_join(y = ncaahoopR::dict, join_by(ncaa_name == name_247))
  }

  {








  }
}


truncate_db(con, force = FALSE)

dbDisconnect(con)
rm(con)
file.remove("dunkdb.ddb")
