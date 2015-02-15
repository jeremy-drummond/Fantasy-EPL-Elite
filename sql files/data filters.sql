use epl;

-- selected by % top players vs overall
select (count(position.id)  / (select count(*) / 15 from position)) * 100 as 'elite_util' , web_name, selected_by_percent, now_cost, total_points
from position, player, gameweekteam 
where player.id = position.id
and gameweekteam.game_week = 24 
and gameweekteam.gameWeekTeamID = position.gameWeekTeamID
and element_type = 2
and started = true
and now_cost < 51
group by position.id
order by elite_util desc, now_cost desc;

-- average player cost by area of team for top players
select avg(now_cost) / 10  as 'average value' from player, position where player.id = position.id and element_type = 3;

-- most captained
select count(captainID) as captained, web_name from gameweekteam, player 
where player.id = captainID
group by captainID
order by captained desc;

-- most vice captained
select count(vice_captainID) as vice_captained, web_name from gameweekteam, player 
where player.id = vice_captainID
group by vice_captainID
order by vice_captained desc;
