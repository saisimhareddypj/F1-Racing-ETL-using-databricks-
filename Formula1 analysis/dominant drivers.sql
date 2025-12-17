-- Databricks notebook source
select * from f1_presentation.calculated_race_results

-- COMMAND ----------

drop view if exists f1_presentation.v_dominant_drivers;
create view f1_presentation.v_dominant_drivers 
as 
select driver_name, 
sum(calculated_points) as total_points,
count(*) as total_races, 
round(avg(calculated_points), 2) as avg_points,
rank() over(order by round(avg(calculated_points), 2) desc) as ranking
from f1_presentation.calculated_race_results
group by driver_name
having total_races > 50
order by ranking asc

-- COMMAND ----------

select * from f1_presentation.v_dominant_drivers

-- COMMAND ----------

with dominant_drivers as 

(select race_year, 
driver_name, 
sum(calculated_points) as total_points,
count(*) as total_races, 
round(avg(calculated_points), 2) as avg_points
from f1_presentation.calculated_race_results
group by driver_name, race_year
order by race_year, avg_points desc)

select * from dominant_drivers
where driver_name in (select driver_name from f1_presentation.v_dominant_drivers where ranking <=10);