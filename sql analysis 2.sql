create database mdb;
use mdb;
CREATE TABLE production (
    Job_ID VARCHAR(10) PRIMARY KEY,
    Machine_ID VARCHAR(5),
    OperationType VARCHAR(20),
    MaterialUsed DECIMAL(5,2),
    ProcessingTime INT,
    EnergyConsumption DECIMAL(5,2),
    MachineAvailability INT,
    ScheduledStart datetime,
    ScheduledEnd datetime,
    ActualStart datetime,
    ActualEnd datetime,
    JobStatus varchar(30),
    Optimization_Category VARCHAR(30)
);

select*from production;
SET GLOBAL LOCAL_INFILE=ON;
load data local infile 'C:/Users/admin/Downloads/production.csv'
INTO TABLE production FIELDS TERMINATED BY','
ignore 1 lines;

-- kpi's 
-- total energy consumed by machines

create table kpi_energy_by_machine_operation as 
select Machine_ID,OperationType,sum(EnergyConsumption) as total_power_use from production group by Machine_ID , OperationType order by Machine_ID , OperationType;
select *from kpi_energy_by_machine_operation;

-- oee --> which machine is good 

create table kpi_oee_by_machine as select machine_id, round(avg(machineAvailability),1) as oee_avg_ma , 
count(*) as total_jobs , 
round(avg(timestampdiff(minute,actualstart , actualend)/timestampdiff(minute,scheduledstart , scheduledend)*100),1) as actual_vs_scheduled 
from production  where ActualStart is not null and ActualEnd is not null
group by machine_id order by oee_avg_ma desc;
select *from kpi_oee_by_machine;

-- efficiency (production efficiency output vs target)

 create table  production_efficiency as select machine_id,count( case when jobstatus ='completed' then 1 end) as complete,
count(*) as total_jobs,round(count(case when jobstatus ='completed' then 1 end )*100/count(*),1) as
 efficiency from production group by machine_id;
 select *from production_efficiency;
 
-- downtime analysis ()

create table downtime_by_machine as 
select machine_id,avg(timestampdiff(minute , scheduledstart,actualstart)) as avg_delay_min,
                  count(case when jobstatus = "Failed" then 1 end ) as failures,
                  count(case when jobstatus = "delayed" then 0 end) as delays from production group by machine_id;
select*from downtime_by_machine;

-- energy efficiency (check the power waste)

create table energy_by_machine as
select machine_id,round(avg(energyconsumption/processingtime),2) as energy_per_min,
avg(processingtime) as avg_processing_time from production group by machine_id order by energy_per_min desc;
select *from energy_by_machine;

-- daily performance trend 

create table daily_trend as
select date(scheduledstart) as date,
avg(machineavailability) as daily_oee,
count(*) as total_jobs,
round(avg(timestampdiff(minute,scheduledstart,actualend)),0) as cycle_time_min from production
group by date(scheduledstart) order by date;
select *from daily_trend;

select *from production_re;
set sql_safe_updates =0;
update production set actualstart = null where actualstart = '0000-00-00 00:00:00' ;
update production set actualend = null where actualend = '0000-00-00 00:00:00' ;
SELECT @@sql_mode;
SET SESSION sql_mode = REPLACE(@@sql_mode, 'NO_ZERO_DATE', '');
SET SESSION sql_mode = REPLACE(@@sql_mode, 'NO_ZERO_IN_DATE', '');
