create database Vehicle_theftdb

-- This database is provided by the Newzealand Police dept, where they are trying to find the insight and pattern of theft
-- so that they can understand the theft and take the measure accordingly also the company who is taking care of their insurance 
-- are also intrested in the analysis to create their policies much more stronger and the company who are the maker can introduce
-- some security features.

use Vehicle_theftdb


--step I- Import all files except the data dictionary

--- we use the Bulk insert query to import the data from the location

--- for the bulk insert you need to create the structured container(table structure) and keep all columns as varchar

create table stolen_vehicles
(vehicle_id varchar(max),
vehicle_type varchar(max),
make_id varchar(max),
model_year varchar(max),
vehicle_desc varchar(max),
color varchar(max),
date_stolen varchar(max),
location_id varchar(max))

select * from stolen_vehicles

-- locations table


create table locations
(location_id varchar(max),
region varchar(max),
country varchar(max),
population varchar(max),
density varchar(max))

select * from locations

--- Make details table

create table make_details
(make_id varchar(max),
make_name varchar(max),
make_type varchar(max))

select * from make_details

select column_name, data_type
from information_schema.columns
where table_name in ('Stolen_vehicles','locations','make_details')

-- Bulk insert for Stolen_vehicles

/* path of files
1. stolen_vehicles - C:\Users\Crossignite.com\Downloads\stolen_vehicles (1)
2. locations- C:\Users\Crossignite.com\Downloads\locations
3. make_details- C:\Users\Crossignite.com\Downloads\make_details */

bulk insert stolen_vehicles
from 'C:\Users\Crossignite.com\Downloads\stolen_vehicles (1).csv'
with ( fieldterminator=',',
rowterminator='\n',
firstrow=2)

-- locations
bulk insert locations
from 'C:\Users\Crossignite.com\Downloads\locations.csv'
with ( fieldterminator=',',
rowterminator='\n',
firstrow=2)

bulk insert make_details
from 'C:\Users\Crossignite.com\Downloads\make_details.csv'
with ( fieldterminator=',',
rowterminator='\n',
firstrow=2)

select * from make_details
select * from locations
select * from stolen_vehicles

--- let's structure the table as per the data

alter table make_details
alter column make_id int
-- this is giving us the error

-- let's check the data in that column

select * from Make_details
where isnumeric(make_id)=0 /* this isnumeric function check the data as numeric format
 other than that with =0 gives the non numeric data*/

 select * from Make_details
where isnumeric(make_id)=0

-- if we need to find the data other than the non numeric format

select * from Make_details
where make_id  not like '[0-9][0-9][0-9]'

update make_details set make_id= case when isnumeric(make_id)=0 then 518 
									when make_id <0 then 522 else make_id end


alter table locations
alter column location_id int


alter table locations
alter column population int

select * from locations


alter table locations
alter column density decimal (7,2)

-- stolen vehicles

select * from stolen_vehicles


alter table stolen_vehicles
alter column make_id int

 select * from stolen_vehicles
where isnumeric(make_id)=0

-- take 5 minutes and change all these non numeric values into (623, 505,543,503)
update stolen_vehicles set make_id = case when make_id like '623%' then 623
										when make_id like '503%' then 503
										when make_id like '505%' then 505
										when make_id like '543%' then 543 else make_id end

update stolen_vehicles set make_id= case when isnumeric(make_id)=0 then 

update stolen_vehicles set make_id = substring(make_id,1,3) where make_id like’%[0-9][0-9][0-9]%’

select make_id,substring(make_id,1,3) from stolen_vehicles where make_id like '%[0-9][0-9][0-9]%'

select make_id,case when make_id like '623%' then 623
										when make_id like '503%' then 503
										when make_id like '505%' then 505
										when make_id like '543%' then 543 else make_id end
as corrected_id from stolen_vehicles

-- copying the data

select * from sv
where isnumeric(make_id)=0

update sv set make_id=substring(make_id,1,3)  where make_id like '%[0-9][0-9][0-9]%'

select * from sv
where vehicle_id=18

--- changing the non numeric values in make_id (stolen_vehicles)

update stolen_vehicles set make_id = substring(make_id,1,3) where make_id like '%[0-9][0-9][0-9]%'

alter table stolen_vehicles
alter column make_id int

alter table stolen_vehicles
alter column model_year int


alter table stolen_vehicles
alter column location_id int


alter table stolen_vehicles
alter column date_stolen date

-- it gives us the conversion error due to non dates values

select * from stolen_vehicles
where isdate(date_stolen)=0

-- correct date format - yyyy-mm-dd
update stolen_vehicles set date_stolen=

select date_stolen, convert(varchar(30), date_stolen,23) as new_date from stolen_vehicles

select date_stolen, case when charindex('/',date_stolen)>0 then
 convert(varchar, convert(date,date_stolen,101),23) else
		getdate()end from stolen_vehicles


-- month 12 in a year ( 

--- yyyy-mm-dd (2021-10-15
--Session 2 continues 22nd March 2024

use Vehicle_theftdb

select date_stolen, convert(varchar, Try_convert(date, date_stolen),23) from stolen_vehicles

update stolen_vehicles set date_stolen = convert(varchar, Try_convert(date, date_stolen),23) from stolen_vehicles


update stolen_vehicles set date_stolen= '2021-10-15'
where date_stolen is null


alter table stolen_vehicles
alter column date_stolen date

select column_name, data_type
from information_schema.columns
where table_name='stolen_vehicles'

select * from stolen_vehicles

-- date stolen
--- we can find the pattern in weekdays and weekends as well, some comparison as per monthly theft, with a particular make type, 

select min(model_year) as 'oldest_model', max(model_year) as latest_model from stolen_vehicles

select distinct model_year from stolen_vehicles
select distinct vehicle_type from stolen_vehicles
select distinct make_type from make_details

--- we can create a comparative study of Stolen_vehicles if there is any monthly trends

select *, datename(month,date_stolen)as months_of_theft from stolen_vehicles
where datename(month,date_stolen) in ('January','february','march') and year(date_stolen)=2022  /*2555*/


select *, datename(month,date_stolen)as months_of_theft from stolen_vehicles
where  year(date_stolen)=2022

select 2555+1669

select count(*) from stolen_vehicles

select *, datename(month,date_stolen)as months_of_theft from stolen_vehicles

select year(date_stolen) as stolen_year
, Month(date_stolen) as stolen_month,
count(*) as stolen_count
from stolen_vehicles
group by year(date_stolen), month(date_stolen)
order by stolen_year, stolen_month


--- we will check the stolen rate by the population

select * from Locations

select * from stolen_vehicles

select distinct vehicle_id from stolen_vehicles /* we can count the vehicle id for stolen vehicle count as they are unique*/

select cast(count(sv.vehicle_id) as float)/sum(l.population)*100 as 'rate',
l.region, count(sv.vehicle_id) as 'total_stolen_vehicles'
from stolen_vehicles Sv
join locations l
on sv.location_id=l.location_id
group by l.region

select l.region, density,count(sv.vehicle_id) as 'Total_theft',
 round(cast(count(sv.vehicle_id) as float)/l.population *100,2) as Svrateper_pop
from stolen_vehicles sv
join locations l
on sv.location_id=l.location_id
group by l.region, l.population, density
order by Svrateper_pop  desc


--- per capita

-- We'll find the regions with the similar stolen vehicle profiles
--we need to create the stolen vehicle profile
select * from stolen_vehicles
select * from Locations
Select * from Make_details

with sv_profile as(select sv.vehicle_id,m.make_name,m.make_type,sv.make_id, model_year,vehicle_type,color, date_stolen, sv.location_id, l.region,l.population, density
from locations l
join stolen_vehicles sv
on l.location_id=sv.location_id
join make_details m
on m.make_id=sv.make_id)

select region, count(vehicle_id) as stolen_count,
				count(distinct make_name) as un_make_name
				, count(distinct color) as uni_color,
				count(distinct model_year) as uni_year,
				round(avg(cast(population as float)),2) as 'scaled_value_pop',
				round(avg(cast(density as float)),2)  'scaled_val_of_density'
from sv_profile
where make_type='Luxury'
group by region
order by region

-- standard--

with sv_profile as(select sv.vehicle_id,m.make_name,m.make_type,sv.make_id, model_year,vehicle_type,color, date_stolen, sv.location_id, l.region,l.population, density
from locations l
join stolen_vehicles sv
on l.location_id=sv.location_id
join make_details m
on m.make_id=sv.make_id)

select region, count(vehicle_id) as stolen_count,
				count(distinct make_name) as un_make_name
				, count(distinct color) as uni_color,
				count(distinct model_year) as uni_year,
				round(avg(cast(population as float)),2) as 'scaled_value_pop',
				round(avg(cast(density as float)),2)  'scaled_val_of_density'
from sv_profile
where make_type='Standard'
group by region
order by region

select count(vehicle_id) from stolen_vehicles
where make_id in (select make_id from make_details where make_type='Luxury')

---

select distinct vehicle_type from stolen_vehicles
select * from stolen_vehicles

select vehicle_type, count(vehicle_id) from stolen_vehicles
group by vehicle_type
order by count(vehicle_id) desc

--- let's first find out the day name

select* , datename(weekday, date_stolen) as 'Day_name'
from stolen_vehicles
--- Weekdays--
select* , datename(weekday, date_stolen) as 'Day_name'
from stolen_vehicles
where datename(weekday, date_stolen) not in ('Saturday','Sunday')

-- weekend
select* , datename(weekday, date_stolen) as 'Day_name'
from stolen_vehicles
where datename(weekday, date_stolen) in ('Saturday','Sunday')

--- try to find the pattern as per the days , create a report as per the stolen vehicle theft happend with the top and bottom ranking as per the days
with ranked_veh_profile as( 
select datename(weekday, date_stolen) as 'Day_name', count(vehicle_id) as 'st_veh_count'
	, rank() over(order by count(vehicle_id) desc) as 'Top_rank'
	, rank() over(order by count(vehicle_id) ) as 'Bottom_rank' from stolen_vehicles
	group by datename(weekday, date_stolen))

select day_name, st_veh_count, case when top_rank<=3 then 'Top' +cast(top_rank as varchar)
									when bottom_rank<=3 then 'Bottom' + cast(top_rank-4 as varchar)
									else 'NA' end as 'ranking'
									from ranked_veh_profile
									order by st_veh_count desc

-- Analysis-- The department,They are particularly interested in knowing
-- if there are any patterns in the time gaps between successive thefts.

--Analysis 2
--create a trigger of stolen veh alert (create a table stolen vehicle alert for date_stolen only) 
-- -- when any new records enter into the stolen vehicles data and stolen_vehicles per capita exceeds the threshold value(which is 0.0001 in that region 
--it should create an entry in stolen_vehicle_alert table for stolen date automatically


