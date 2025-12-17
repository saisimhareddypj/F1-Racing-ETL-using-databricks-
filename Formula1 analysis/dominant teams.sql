-- Databricks notebook source
select * from f1_presentation.calculated_race_results

-- COMMAND ----------

drop view if exists f1_presentation.v_dominant_teams;
create view f1_presentation.v_dominant_teams 
as
select team_name, 
round(sum(calculated_points), 2) as total_points,
count(*) as total_races, 
round(avg(calculated_points), 2) as avg_points,
rank() over(order by round(avg(calculated_points), 2) desc) as ranking
from f1_presentation.calculated_race_results
group by team_name
having total_races > 100
order by ranking asc

-- COMMAND ----------

select * from f1_presentation.v_dominant_teams

-- COMMAND ----------

with dominant_teams as (
select race_year, 
team_name, 
round(sum(calculated_points), 2) as total_points,
count(*) as total_races, 
round(avg(calculated_points), 2) as avg_points
from f1_presentation.calculated_race_results
group by race_year, team_name
order by race_year, avg_points desc)

select * from dominant_teams
where team_name in (select team_name from f1_presentation.v_dominant_teams where ranking <= 5)
