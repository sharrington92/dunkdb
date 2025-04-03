#' Create DunkDB Table: teams dimension
#'
#' @returns
#'
#' @examples
create_table_teams <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS teams (
      team_id INT PRIMARY KEY,
      season_id INT,
      school_id INT,
      coach_id INT,
      conf_id INT,
      division TEXT,
      wins INT,
      losses INT,
      ties INT,
      FOREIGN KEY (season_id) REFERENCES seasons (season_id),
      FOREIGN KEY (school_id) REFERENCES schools (school_id),
      FOREIGN KEY (coach_id) REFERENCES coaches (coach_id),
      FOREIGN KEY (conf_id) REFERENCES conferences (conf_id)
    )"
  )

}
