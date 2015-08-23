use epl;

select count(*) from gameweekteam where game_week = 28;

-- selected by % top players vs overall
select (count(position.id)  / (select count(*) from gameweekteam where game_week = 25)) * 100 as 'elite_util',
	count(position.id) as count, web_name, selected_by_percent, now_cost, total_points, gameweekteam.game_week,
    minutes/total_points as min_per_point, points_per_game, transfers_in - transfers_out as net_xfer
from position, player, gameweekteam 
where player.id = position.id
and gameweekteam.game_week = 28 
and gameweekteam.gameWeekTeamID = position.gameWeekTeamID
and element_type = 3
-- and started = true
-- and now_cost < 51
group by position.id
order by elite_util desc, now_cost desc;

-- average player cost by area of team for top players
select avg(now_cost) / 10  as 'average value' from player, position where player.id = position.id and element_type = 3;

-- most captained
select count(captainID) as captained, web_name from gameweekteam, player 
where player.id = captainID
and gameweekteam.game_week = 28
group by captainID
order by captained desc;

-- most vice captained
select count(vice_captainID) as vice_captained, web_name from gameweekteam, player 
where player.id = vice_captainID
and gameweekteam.game_week = 28
group by vice_captainID
order by vice_captained desc;

select minutes/total_points as min_per_point, points_per_game, web_name, transfers_in - transfers_out as net_xfer from player
where minutes > 500 
and event_total > 0
-- and element_type = 3
order by min_per_point asc;

select * from player where web_name like 'Silva';
