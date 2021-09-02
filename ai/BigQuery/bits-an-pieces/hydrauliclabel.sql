--
-- add columns to identify cycle and cycle sequence
--
create or replace view `osi-pi-gcp-accelerator.hydraulic.hydraulicbi` as
select CAST(FORMAT_TIMESTAMP("%d%H%M",TimeStamp) as NUMERIC) as cycle_id,
       CAST(EXTRACT(second FROM TimeStamp) + 1 as NUMERIC) as sequence,
       *
from `osi-pi-gcp-accelerator.hydraulic.hydraulic`
order by TimeStamp
