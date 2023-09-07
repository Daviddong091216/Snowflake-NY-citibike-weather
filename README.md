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
