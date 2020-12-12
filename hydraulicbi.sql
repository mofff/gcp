--
-- create a view of the hydraulic dataset labels for use in Power BI
--
create or replace view `osi-pi-gcp-accelerator.hydraulic.hydrauliclabel` as
with data as (
select TimeStamp, Cooler_Condition, Hydraulic_Accumulator, Internal_Pump_Leakage, Stable, Valve_Condition
from `osi-pi-gcp-accelerator.hydraulic.hydraulic`
where CAST(FORMAT_TIMESTAMP("%S",TimeStamp) as NUMERIC) = 0
order by TimeStamp)
select row_number() over() as cycle,
       CAST(FORMAT_TIMESTAMP("%d%H%M",TimeStamp) as NUMERIC) as cycle_id,
       *
from data
