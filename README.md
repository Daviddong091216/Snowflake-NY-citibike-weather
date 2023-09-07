What I've covered:
How to create stages, databases, tables, views, and virtual warehouses.
How to load structured and semi-structured data.
How to perform analytical queries on data in Snowflake, including joins between tables.
How to clone objects.
How to undo user errors using Time Travel.
How to create roles and users, and grant them privileges.
How to securely and easily share data with other accounts.
How to consume datasets in the Snowflake Data Marketplace.


# Snowflake-NY-citibike-weather
9/7/2023,Snowflake project
use role sysadmin;
use warehouse compute_wh;

create database citibike;

create or replace table trips
(tripduration integer,
starttime timestamp,
stoptime timestamp,
start_station_id integer,
start_station_name string,
start_station_latitude float,
start_station_longitude float,
end_station_id integer,
end_station_name string,
end_station_latitude float,
end_station_longitude float,
bikeid integer,
membership_type string,
usertype string,
birth_year integer,
gender integer);

select * from trips;

create stage citibike_trips
url=s3://snowflake-workshop-lab/citibike-trips-csv/;

list @citibike_trips;

CREATE FILE FORMAT CSV 
TYPE = 'CSV' 
FIELD_DELIMITER = ','
FIELD_OPTIONALLY_ENCLOSED_BY = '"'
COMPRESSION = 'AUTO'; 
--create file format

create or replace file format csv 
  type='csv'
  compression = 'auto' 
  field_delimiter = ',' 
  record_delimiter = '\n'
  skip_header = 0 
  field_optionally_enclosed_by = '\042' 
  trim_space = false
  error_on_column_count_mismatch = false 
  escape = 'none' 
  escape_unenclosed_field = '\134'
  date_format = 'auto' 
  timestamp_format = 'auto' 
  null_if = ('') 
  comment = 'file format for ingesting data for zero to snowflake';

  --verify file format is created

show file formats in database citibike;

copy into trips 
from @citibike_trips 
file_format=csv 
PATTERN = '.*csv.*' ;

select * from trips;

truncate table trips;
--verify table is clear
select * from trips limit 10;
--change warehouse size from small to large (4x)
alter warehouse compute_wh set warehouse_size='large';
--load data with large warehouse
show warehouses;
copy into trips 
from @citibike_trips 
file_format=csv 
PATTERN = '.*csv.*' ;
select * from trips limit 10;

select date_trunc('hour', starttime) as "date",
count(*) as "num trips",
avg(tripduration)/60 as "avg duration (mins)",
avg(haversine(start_station_latitude, start_station_longitude, end_station_latitude, end_station_longitude)) as "avg distance (km)"
from trips
group by 1 order by 1;


select
monthname(starttime) as "month",
count(*) as "num trips"
from trips
group by 1 order by 2 desc;

create table trips_dev clone trips;


create database weather;

use role sysadmin;

use warehouse compute_wh;

use database weather;

use schema public;

create table json_weather_data (v variant);
create stage nyc_weather
url = 's3://snowflake-workshop-lab/zero-weather-nyc';

list @nyc_weather;

copy into json_weather_data
from @nyc_weather 
file_format = (type = json strip_outer_array = true);

select * from json_weather_data limit 10;
// create a view that will put structure onto the semi-structured data
create or replace view json_weather_data_view as
select
    v:obsTime::timestamp as observation_time,
    v:station::string as station_id,
    v:name::string as city_name,
    v:country::string as country,
    v:latitude::float as city_lat,
    v:longitude::float as city_lon,
    v:weatherCondition::string as weather_conditions,
    v:coco::int as weather_conditions_code,
    v:temp::float as temp,
    v:prcp::float as rain,
    v:tsun::float as tsun,
    v:wdir::float as wind_dir,
    v:wspd::float as wind_speed,
    v:dwpt::float as dew_point,
    v:rhum::float as relative_humidity,
    v:pres::float as pressure
from
    json_weather_data
where
    station_id = '72502';


select * from json_weather_data_view
where date_trunc('month',observation_time) = '2018-01-01'
limit 20;

select weather_conditions as conditions
,count(*) as num_trips
from citibike.public.trips
left outer join json_weather_data_view
on date_trunc('hour', observation_time) = date_trunc('hour', starttime)
where conditions is not null
group by 1 order by 2 desc;
drop table json_weather_data;

select * from json_weather_data limit 10;

undrop table json_weather_data;

--verify table is undropped

select * from json_weather_data limit 10;

use role sysadmin;

use warehouse compute_wh;

use database citibike;

use schema public;

update trips set start_station_name = 'oops';

select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;

set query_id =
(select query_id from table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time desc limit 1);

create or replace table trips as
(select * from trips before (statement => $query_id));

select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;
--9. Working with Roles, Account Admin, & Account Usage

use role accountadmin;
create role junior_dba;
grant role junior_dba to user admin;

use role junior_dba;

use role accountadmin;
grant usage on warehouse compute_wh to role junior_dba;

use role junior_dba;
use warehouse compute_wh;

use role accountadmin;

grant usage on database citibike to role junior_dba;

grant usage on database weather to role junior_dba;
use role junior_dba;

use role accountadmin;

--10. Sharing Data Securely & the Data Marketplace

// Get total case count by country
/*
Calculates the total number of cases by country, aggregated over time.
*/
SELECT   COUNTRY_REGION, SUM(CASES) AS Cases
FROM     ECDC_GLOBAL
GROUP BY COUNTRY_REGION;

// Change in mobility in over time
/*
Displays the change in visits to places like grocery stores and parks by date, location and location type for a sub-region (Alexandria) of a state (Virginia) of a country (United States).
*/
SELECT DATE,
       COUNTRY_REGION,
       PROVINCE_STATE,
       GROCERY_AND_PHARMACY_CHANGE_PERC,
       PARKS_CHANGE_PERC,
       RESIDENTIAL_CHANGE_PERC,
       RETAIL_AND_RECREATION_CHANGE_PERC,
       TRANSIT_STATIONS_CHANGE_PERC,
       WORKPLACES_CHANGE_PERC
FROM   GOOG_GLOBAL_MOBILITY_REPORT
WHERE  COUNTRY_REGION = 'United States'
  AND PROVINCE_STATE = 'Virginia'
  AND SUB_REGION_2 = 'Alexandria';

// Date-dependent case fatality ratio
/*
Calculate case-fatality ratio for a given date
*/
SELECT m.COUNTRY_REGION, m.DATE, m.CASES, m.DEATHS, m.DEATHS / m.CASES as CFR
FROM (SELECT COUNTRY_REGION, DATE, AVG(CASES) AS CASES, AVG(DEATHS) AS DEATHS
      FROM ECDC_GLOBAL
      GROUP BY COUNTRY_REGION, DATE) m
WHERE m.CASES > 0;

--11. Resetting Your Snowflake Environment
use role accountadmin;
drop share if exists zero_to_snowflake_shared_data;
-- If necessary, replace "zero_to_snowflake-shared_data" with the name you used for the share

drop database if exists citibike;

drop database if exists weather;

drop warehouse if exists analytics_wh;

drop role if exists junior_dba;
