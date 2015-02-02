drop database epl;

create database epl;

use epl;

create table Manager
(
managerID integer PRIMARY KEY,
season int,
name varchar(50) NOT NULL,
club varchar(50),
team_name varchar(40),
country varchar(50)
);


create table Finance
(
game_week int,
season int,
total_transfers int,
week_transfers int,
wildcard_available bit,
worth decimal(5,2),
bank decimal (5,2),
managerID int,
foreign key (managerID) references Manager (managerID) 
);


create table Player
(
id integer PRIMARY KEY,
web_name varchar(30),
game_week int,
season int,
event_total int,
type_name varchar(10),
team_name varchar(35),
selected_by decimal(5,2),
total_points int,
team_code int,
news varchar(300),
team_id int,
first_name varchar(50),
second_name varchar(50),
now_cost int,
chance_of_playing_this_round int,
chance_of_playing_next_round int,
value_form decimal(5,2),
value_season decimal(5,2),
in_dreamteam bit,
dreamteam_count int,
selected_by_percent decimal(5,2),
form decimal(5,2),
transfers_out int,
transfers_in int,
points_per_game decimal(5,2),
minutes int,
goals_scored int,
assists int,
clean_sheets int,
goals_conceded int,
own_goals int,
penalties_saved int,
penalties_missed int,
yellow_cards int,
red_cards int,
saves int,
bonus int,
ea_index int,
bps int,
element_type int,
team int
);

create table GameWeekTeam
(
gameWeekTeamID varchar(15) PRIMARY KEY,
game_week int,
season int,
overall_points int,
overall_rank int,
game_week_points int,
captainID int, 
vice_captainID int, 
managerID int,
foreign key (captainID) references Player (id),
foreign key (vice_captainID) references Player (id),
foreign key (managerID) references Manager (managerID) 
);

create table Position
(
positionID integer NOT NULL auto_increment PRIMARY KEY,
id int,
gameWeekTeamID varchar(15),
started bit,
foreign key (id) references Player(id),
foreign key (gameWeekTeamID) references GameWeekTeam(gameWeekTeamID)
);