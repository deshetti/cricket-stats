-- DROP USER cricstats;

CREATE USER cricstats WITH
  LOGIN
  SUPERUSER
  INHERIT
  CREATEDB
  CREATEROLE
  NOREPLICATION;

-- SCHEMA: cricstats

CREATE SCHEMA cricstats
  AUTHORIZATION cricstats;

-- Table: icc_ranking
CREATE TABLE cricstats.icc_ranking (
  id bigserial  NOT NULL,
  full_name varchar(100)  NOT NULL,
  rating int  NOT NULL,
  ranking int  NOT NULL,
  date date  NOT NULL,
  country varchar(100)  NOT NULL,
  sex varchar(20)  NOT NULL,
  format varchar(20)  NOT NULL,
  icc_id int  NOT NULL,
  ranking_type varchar(20)  NOT NULL,
  CONSTRAINT icc_ranking_pk PRIMARY KEY (id)
);
-- End of file.

-- Table: ci_matches
CREATE TABLE cricstats.ci_match (
  id bigserial  NOT NULL,
  team1 varchar(150)  NULL,
  team1_ciid int  NULL,
  team2 varchar(150)  NULL,
  team2_ciid int  NULL,
  winner varchar(50)  NULL,
  winner_ciid int  NULL,
  margin varchar(100)  NULL,
  ground varchar(150)  NULL,
  ground_ciid int  NULL,
  start_date date  NOT NULL,
  end_date date  NOT NULL,
  match_type varchar(20)  NOT NULL,
  match_type_ciid int  NULL,
  match_number int  NULL,
  match_ciid int  NOT NULL,
  CONSTRAINT ci_matches_pk PRIMARY KEY (id)
);
-- End of file.

-- Table: ci_international_teams
CREATE TABLE cricstats.ci_international_team (
  id serial  NOT NULL,
  name varchar(100)  NOT NULL,
  ciid int  NOT NULL,
  last_updated timestamp NOT NULL,
  CONSTRAINT ci_international_teams_pk PRIMARY KEY (id)
);
-- End of file.

-- Table: player
CREATE TABLE cricstats.player (
  id bigserial  NOT NULL,
  ci_name varchar(100)  NOT NULL,
  ciid int  NOT NULL,
  full_name varchar(100)  NULL,
  international_team_ciid int  NULL,
  nick_name varchar  NULL,
  dob date  NULL,
  born_city varchar(100)  NULL,
  major_teams varchar  NULL,
  playing_role varchar(100)  NULL,
  batting_style varchar(100)  NULL,
  bowling_style varchar(100)  NULL,
  height varchar(10)  NULL,
  education varchar(250)  NULL,
  icc_id int  NULL,
  last_updated timestamp  NOT NULL,
  country varchar(100)  NULL,
  CONSTRAINT player_pk PRIMARY KEY (id)
);
-- End of file.