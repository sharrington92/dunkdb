#' Create DunkDB Database Tables
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_tables <- function(con){

  create_table_name_mapping(con)
  create_table_schools(con)
  create_table_seasons(con)
  create_table_conferences(con)
  create_table_coaches(con)
  create_table_teams(con)
  create_table_rosters(con)
  create_table_players(con)
  create_table_games(con)
  create_table_boxscores(con)
  create_table_events(con)
  create_table_events_mapping(con)
  create_table_event_tags(con)
  create_table_pbp(con)

}


#' Create DunkDB Table: boxscores fact
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_boxscores <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS box (
      game_id INT,
      player_id INT,
      tech INT,
      fouls INT,
      blocks INT,
      steals INT,
      turnover INT,
      assist INT,
      rebound_total INT,
      rebound_def INT,
      rebound_off INT,
      points INT,
      freethrow_pct DECIMAL(6, 3),

      FOREIGN KEY (game_id) REFERENCES games (game_id),
      FOREIGN KEY (player_id) REFERENCES players (player_id)
    )"
  )

}

#' Create DunkDB Table: coaches dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_coaches <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS coaches (
      coach_id INT PRIMARY KEY,
      name TEXT
    )"
  )

}

#' Create DunkDB Table: conferences dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_conferences <- function(con){

  DBI::dbExecute(
    con,
    "
    CREATE TABLE IF NOT EXISTS conferences (
      conf_id INT PRIMARY KEY,
      conf TEXT,
      conf_type TEXT,
      is_current BOOL
    )
    "
  )

}


#' Create DunkDB Table: events dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_events <- function(con){

  DBI::dbExecute(
    con,
    "CREATE SEQUENCE IF NOT EXISTS event_id_seq START 1;
    CREATE TABLE IF NOT EXISTS events (
    	event_id INT DEFAULT nextval('event_id_seq') PRIMARY KEY,
    	event TEXT,
    	event_type TEXT,
    	event_desc TEXT,

    	UNIQUE(event)
    );"
  )

}

#' Create DunkDB Table: event_tags dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_event_tags <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS event_tags (
      tag TEXT PRIMARY KEY
    );"
  )

}


#' Create DunkDB Table: events_mapping dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_events_mapping <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS events_map (
      event_raw TEXT,
    	event TEXT,

    	UNIQUE(event_raw)
    );"
  )

}


#' Create DunkDB Table: games fact
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_games <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS games (
      game_id INT PRIMARY KEY,
      box_id INT NOT NULL,
      start_dttm TIMESTAMP,
      home_team_id INT NOT NULL,
      away_team_id INT NOT NULL,
      home_score INT,
      away_score INT,
      attendance INT,
      neutral_site BOOL,

      FOREIGN KEY (home_team_id) REFERENCES teams (team_id),
      FOREIGN KEY (away_team_id) REFERENCES teams (team_id)
    )"
  )

}


#' Create DunkDB Table: name_mapping dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_name_mapping <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS name_mapping (
    	name TEXT,
    	nickname TEXT,
    	UNIQUE (name, nickname)
    )"
  )

}


#' Create DunkDB Table: play-by-play dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_pbp <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS play_by_play (
    	game_id INT,
    	game_event_id INT,
    	period SMALLINT,
    	seconds_elapsed DECIMAL,
    	score_home INT,
    	score_away INT,
    	team_id INT,
    	player_id INT,
    	event_result NVARCHAR(25),
    	event_tags NVARCHAR(100),
    	event TEXT,

    	UNIQUE(game_id, game_event_id),
    	FOREIGN KEY (team_id) REFERENCES teams (team_id),
    	FOREIGN KEY (player_id) REFERENCES players (player_id)
    );"
  )

}


#' Create DunkDB Table: players dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_players <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS players (
      player_id INT PRIMARY KEY,
      name TEXT,
      name_last TEXT,
      name_first TEXT,
      suffix TEXT,
      hometown TEXT,
      high_school TEXT
    )"
  )

}


#' Create DunkDB Table: rosters dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_rosters <- function(con){

  DBI::dbExecute(
    con,
    "CREATE SEQUENCE IF NOT EXISTS roster_id_seq START 1;
    CREATE TABLE IF NOT EXISTS rosters (
    	roster_id INT DEFAULT nextval('roster_id_seq') PRIMARY KEY,
    	team_id INT,
    	player_id INT,
    	games_play INT,
    	games_start INT,
    	jersey INT,
    	class TEXT,
    	position TEXT,
    	height_in DECIMAL(6, 3)

    --	UNIQUE(roster_id),
    	--PRIMARY KEY (team_id, player_id),
    --	UNIQUE(team_id, player_id),
    --	FOREIGN KEY (player_id) REFERENCES players (player_id),
    --	FOREIGN KEY (team_id) REFERENCES teams (team_id)
    );"
  )

}


#' Create DunkDB Table: schools dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_schools <- function(con){

  DBI::dbExecute(
    con,
    "
    CREATE TABLE IF NOT EXISTS schools (
      school_id INT PRIMARY KEY,
      espn_id INT,
      ncaa_name TEXT,
      espn_name TEXT,
      espn_abbrv TEXT,
      primary_color TEXT,
      secondary_color TEXT,
      tertiary_color TEXT,
      color_4 TEXT,
      color_5 TEXT,
      color_6 TEXT,
      logo_url TEXT,
  	espn_link TEXT
    )
    "
  )

}


#' Create DunkDB Table: seasons dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_table_seasons <- function(con){

  DBI::dbExecute(
    con,
    "CREATE TABLE IF NOT EXISTS seasons (
      season_id INT PRIMARY KEY,
      season TEXT,
      academic_year INT
    )"
  )

}


#' Create DunkDB Table: teams dimension
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
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


#' Create DunkDB Staging Tables
#'
#' @param con A DBI database connection object to the dunkdb DuckDB database.
#'
#' @returns N/A. Executes SQL to create database table.
#'
create_tables_staging <- function(con){

  DBI::dbExecute(
    con,
    "
    CREATE TABLE IF NOT EXISTS staging_conf (
      conf_id TEXT,
      conf TEXT,
      academic_year INT
    );


    CREATE TABLE IF NOT EXISTS staging_teams (
      team_id TEXT,
      season TEXT,
      school_id TEXT,
      coach_id TEXT,
      conference TEXT,
      division TEXT,
      wins TEXT,
      losses TEXT,
      ties TEXT
    );
    -- This populates: coaches, teams


    CREATE TABLE IF NOT EXISTS staging_rosters (
      team_id INT,
      player_id TEXT,
      player_name TEXT,
      games_played TEXT,
      games_start TEXT,
      jersey TEXT,
      class TEXT,
      position TEXT,
      height TEXT,
      hometown TEXT,
      high_school TEXT
    );
    -- This populates: players, rosters


    CREATE TABLE IF NOT EXISTS staging_games (
      game_id TEXT,
      box_id TEXT,
      date DATE,
      start_time TEXT,
      home TEXT,
      away TEXT,
      home_score TEXT,
      away_score TEXT,
      attendance TEXT,
      neutral_site TEXT,
      home_win TEXT,
      home_loss TEXT,
      away_win TEXT,
      away_loss TEXT
    );


    CREATE TABLE IF NOT EXISTS staging_pbp (
      time TEXT,
      event_team_away TEXT,
      score TEXT,
      event_team_home TEXT,
      period TEXT,
      team_home TEXT,
      team_away TEXT,
      game_id INT
    );
    "
  )

}


