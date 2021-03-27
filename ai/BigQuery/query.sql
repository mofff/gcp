/* 
 
# add TO an existing TABLE
ALTER TABLE
  `osi-pi-gcp-accelerator.hydraulic.hydraulic` add column prediction STRUCT<stable FLOAT64,
  cooler float64,
  accumulator float64,
  pump_leakage float64,
  valve float64>; 
  
# CREATE NEW TABLE
CREATE TABLE
  `osi-pi-gcp-accelerator.hydraulic.hydraulic_predictions` ( timestamp timestamp,
    prediction STRUCT< stable FLOAT64,
    cooler float64,
    accumulator float64,
    pump_leakage float64,
    valve float64> );

SELECT
  *
FROM
  `osi-pi-gcp-accelerator.hydraulic.hydraulic`
ORDER BY
  timestamp DESC
LIMIT
  60; # summarize DATA BY cycle tests

SELECT
  CAST(FORMAT_TIMESTAMP("%d%H%M",TimeStamp) AS NUMERIC) AS cycle_id,
  COUNT(Pressure1)
FROM
  `osi-pi-gcp-accelerator.hydraulic.hydraulic`
GROUP BY
  cycle_id
ORDER BY
  cycle_id

SELECT
  CAST(FORMAT_TIMESTAMP("%d%H%M",TimeStamp) AS NUMERIC) AS cycle_id,
  AVG(pressure1) AS pressure1,
  COUNT(pressure1) AS count,
  EXTRACT(day
  FROM
    TimeStamp) AS date_part
FROM
  `osi-pi-gcp-accelerator.hydraulic.hydraulic`
WHERE
  cycle_id < '2020-03-26'
GROUP BY
  cycle_id
ORDER BY
  cycle_id DESC

SELECT
  datetime_TRUNC(current_datetime(),
    DAY) AS date_trunc,
  datetime_sub(datetime_TRUNC(current_datetime(),
      DAY),
    INTERVAL 2206 MINUTE) AS date_start

SELECT
  FORMAT_TIMESTAMP("%d%H%M",CURRENT_TIMESTAMP()) # determine the period which describes the entire dataset (approx. 1.5 days) - 2205 minutes
WITH
  dates AS (
  SELECT
    datetime_TRUNC(current_datetime(),
      DAY) AS datetime_end,
    datetime_sub(datetime_TRUNC(current_datetime(),
        DAY),
      INTERVAL 2205 MINUTE) AS datetime_start )

SELECT
  COUNT(*),
  CAST(FORMAT_TIMESTAMP("%d%H%M",TimeStamp) AS NUMERIC) AS cycle_id,
  AVG(Cooler_Condition) AS Cooler_Condition,
  AVG(Cooling_Efficiency) AS Cooling_Efficiency,
  AVG(Cooling_Power) AS Cooling_Power,
  AVG(Efficiency_Factor) AS Efficiency_Factor,
  AVG(Hydraulic_Accumulator) AS Hydraulic_Accumulator,
  AVG(Internal_Pump_Leakage) AS Internal_Pump_Leakage,
  AVG(Motor_Power) AS Motor_Power,
  AVG(Pressure1) AS Pressure1,
  AVG(Pressure2) AS Pressure2,
  AVG(Pressure3) AS Pressure3,
  AVG(Pressure4) AS Pressure4,
  AVG(Pressure5) AS Pressure5,
  AVG(Pressure6) AS Pressure6,
  AVG(Stable) AS Stable,
  AVG(Temperature1) AS Temperature1,
  AVG(Temperature2) AS Temperature2,
  AVG(Temperature3) AS Temperature3,
  AVG(Temperature4) AS Temperature4,
  AVG(Valve_Condition) AS Valve_Condition,
  AVG(Vibration) AS Vibration,
  AVG(Volume_Flow1) AS Volume_Flow1,
  AVG(Volume_Flow2) AS Volume_Flow2
FROM
  dates,
  `osi-pi-gcp-accelerator.hydraulic.hydraulic`
WHERE
  CAST(Timestamp AS datetime) > datetime_start
  AND CAST(Timestamp AS datetime) < datetime_end
GROUP BY
  cycle_id
ORDER BY
  cycle_id; 
  
# DATA SET period 
# 2021-03-25T11:15:00 
# 2021-03-27T00:00:00
SELECT
  datetime_TRUNC(current_datetime(),
    DAY) AS datetime_end,
  datetime_sub(datetime_TRUNC(current_datetime(),
      DAY),
    INTERVAL 2205 MINUTE) AS datetime_start 
    
# view simplifies model building inputby predefining input dataset

CREATE VIEW
  `osi-pi-gcp-accelerator.hydraulic.hydraulicds` OPTIONS( friendly_name="hydraulicds",
    description="dataset to use for model building",
    labels=[("dataset_type",
      "batch")] ) AS
SELECT
  COUNT(*) AS count,
  CAST(FORMAT_TIMESTAMP("%d%H%M",TimeStamp) AS NUMERIC) AS cycle_id,
  AVG(Cooler_Condition) AS Cooler_Condition,
  AVG(Cooling_Efficiency) AS Cooling_Efficiency,
  AVG(Cooling_Power) AS Cooling_Power,
  AVG(Efficiency_Factor) AS Efficiency_Factor,
  AVG(Hydraulic_Accumulator) AS Hydraulic_Accumulator,
  AVG(Internal_Pump_Leakage) AS Internal_Pump_Leakage,
  AVG(Motor_Power) AS Motor_Power,
  AVG(Pressure1) AS Pressure1,
  AVG(Pressure2) AS Pressure2,
  AVG(Pressure3) AS Pressure3,
  AVG(Pressure4) AS Pressure4,
  AVG(Pressure5) AS Pressure5,
  AVG(Pressure6) AS Pressure6,
  AVG(Stable) AS Stable,
  AVG(Temperature1) AS Temperature1,
  AVG(Temperature2) AS Temperature2,
  AVG(Temperature3) AS Temperature3,
  AVG(Temperature4) AS Temperature4,
  AVG(Valve_Condition) AS Valve_Condition,
  AVG(Vibration) AS Vibration,
  AVG(Volume_Flow1) AS Volume_Flow1,
  AVG(Volume_Flow2) AS Volume_Flow2
FROM
  `osi-pi-gcp-accelerator.hydraulic.hydraulic`
WHERE
  CAST(Timestamp AS datetime) > "2021-03-25T11:15:00"
  AND CAST(Timestamp AS datetime) < "2021-03-27T00:00:00"
GROUP BY
  cycle_id
ORDER BY
  cycle_id;


SELECT
  DISTINCT(cooler_condition),
  COUNT(*)
FROM
  osi-pi-gcp-accelerator.hydraulic.hydraulicds
GROUP BY
  cooler_condition 
  
# CREATE a model USING the previous view AS input 
CREATE MODEL
  `osi-pi-gcp-accelerator.hydraulic.hydraulic_model_xg` TRANSFORM ( Cooler_Condition,
    Cooling_Efficiency,
    Cooling_Power,
    Efficiency_Factor,
    Hydraulic_Accumulator,
    Internal_Pump_Leakage,
    Motor_Power,
    Pressure1,
    Pressure2,
    Pressure3,
    Pressure4,
    Pressure5,
    Pressure6,
    Stable,
    Temperature1,
    Temperature2,
    Temperature3,
    Temperature4,
    Valve_Condition,
    Vibration,
    Volume_Flow1,
    Volume_Flow2,
    ) OPTIONS(MODEL_TYPE='BOOSTED_TREE_CLASSIFIER',
    BOOSTER_TYPE = 'GBTREE',
    NUM_PARALLEL_TREE = 1,
    MAX_ITERATIONS = 50,
    TREE_METHOD = 'HIST',
    EARLY_STOP = FALSE,
    SUBSAMPLE = 0.85,
    INPUT_LABEL_COLS = ['cooler_condition']) AS
SELECT
  *
FROM
  `osi-pi-gcp-accelerator.hydraulic.hydraulicds`; 
  
# EXPORT model - does NOT work because transform used IN
CREATE MODEL
  statement EXPORT MODEL `osi-pi-gcp-accelerator.hydraulic.hydraulic_model_xg` OPTIONS(URI = 'gs://hydraulic/model/hydraulic_model_xg') 
  
# determine the period which describes the entire dataset FROM midnight TO midnight - 2205 minutes
WITH
  dates AS (
  SELECT
    datetime_TRUNC(current_datetime(),
      DAY) AS datetime_end,
    datetime_sub(datetime_TRUNC(current_datetime(),
        DAY),
      INTERVAL 2205 MINUTE) AS datetime_start )
SELECT
  COUNT(*),
  CAST(FORMAT_TIMESTAMP("%d%H%M",TimeStamp) AS NUMERIC) AS cycle_id,
  AVG(Cooler_Condition) AS Cooler_Condition,
  AVG(Cooling_Efficiency) AS Cooling_Efficiency,
  AVG(Cooling_Power) AS Cooling_Power,
  AVG(Efficiency_Factor) AS Efficiency_Factor,
  AVG(Hydraulic_Accumulator) AS Hydraulic_Accumulator,
  AVG(Internal_Pump_Leakage) AS Internal_Pump_Leakage,
  AVG(Motor_Power) AS Motor_Power,
  AVG(Pressure1) AS Pressure1,
  AVG(Pressure2) AS Pressure2,
  AVG(Pressure3) AS Pressure3,
  AVG(Pressure4) AS Pressure4,
  AVG(Pressure5) AS Pressure5,
  AVG(Pressure6) AS Pressure6,
  AVG(Stable) AS Stable,
  AVG(Temperature1) AS Temperature1,
  AVG(Temperature2) AS Temperature2,
  AVG(Temperature3) AS Temperature3,
  AVG(Temperature4) AS Temperature4,
  AVG(Valve_Condition) AS Valve_Condition,
  AVG(Vibration) AS Vibration,
  AVG(Volume_Flow1) AS Volume_Flow1,
  AVG(Volume_Flow2) AS Volume_Flow2
FROM
  dates,
  `osi-pi-gcp-accelerator.hydraulic.hydraulic`
WHERE
  CAST(Timestamp AS datetime) > datetime_start
  AND CAST(Timestamp AS datetime) < datetime_end
GROUP BY
  cycle_id
ORDER BY
  cycle_id; 
  
# view setup TO simplify model execution, query supplies dates FOR dataset TO feed TO model

CREATE OR REPLACE VIEW
  `osi-pi-gcp-accelerator.hydraulic.hydraulicv` OPTIONS( friendly_name="hydraulicv",
    description="shape data for input to model prediction",
    labels=[("dataset_type",
      "batch")] ) AS
SELECT
  COUNT(*) AS count,
  CAST(FORMAT_TIMESTAMP("%d%H%M",TimeStamp) AS NUMERIC) AS cycle_id,
  MIN(TIMESTAMP_TRUNC(TimeStamp,MINUTE)) AS cycle,
  AVG(Cooler_Condition) AS Cooler_Condition,
  AVG(Cooling_Efficiency) AS Cooling_Efficiency,
  AVG(Cooling_Power) AS Cooling_Power,
  AVG(Efficiency_Factor) AS Efficiency_Factor,
  AVG(Hydraulic_Accumulator) AS Hydraulic_Accumulator,
  AVG(Internal_Pump_Leakage) AS Internal_Pump_Leakage,
  AVG(Motor_Power) AS Motor_Power,
  AVG(Pressure1) AS Pressure1,
  AVG(Pressure2) AS Pressure2,
  AVG(Pressure3) AS Pressure3,
  AVG(Pressure4) AS Pressure4,
  AVG(Pressure5) AS Pressure5,
  AVG(Pressure6) AS Pressure6,
  AVG(Stable) AS Stable,
  AVG(Temperature1) AS Temperature1,
  AVG(Temperature2) AS Temperature2,
  AVG(Temperature3) AS Temperature3,
  AVG(Temperature4) AS Temperature4,
  AVG(Valve_Condition) AS Valve_Condition,
  AVG(Vibration) AS Vibration,
  AVG(Volume_Flow1) AS Volume_Flow1,
  AVG(Volume_Flow2) AS Volume_Flow2
FROM
  `osi-pi-gcp-accelerator.hydraulic.hydraulic`
GROUP BY
  cycle_id
ORDER BY
  cycle_id; 

# explore running model
WITH
  inference AS (
  SELECT
    cycle,
    predicted_cooler_condition
  FROM
    ML.PREDICT(MODEL `osi-pi-gcp-accelerator.hydraulic.hydraulic_model_xg`,
      (
      SELECT
        *
      FROM
        `osi-pi-gcp-accelerator.hydraulic.hydraulicv`
      WHERE
        cycle_id < CAST(FORMAT_TIMESTAMP("%d%H%M",TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)) AS NUMERIC)
        AND cycle_id > CAST(FORMAT_TIMESTAMP("%d%H%M",TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 MINUTE)) AS NUMERIC))) AS model ) 
        
# Cannot UPDATE records - add inference results TO input TABLE because: 
# UPDATE   OR DELETE statement OVER TABLE osi-pi-gcp-accelerator.hydraulic.hydraulic would affect ROWS IN the streaming buffer,which IS NOT supported 
INSERT
  `osi-pi-gcp-accelerator.hydraulic.hydraulic_predictions` (timestamp,
    prediction)
UPDATE
  `osi-pi-gcp-accelerator.hydraulic.hydraulic`
SET
  prediction.cooler = predicted_cooler_condition
SELECT
  cycle,
  STRUCT(predicted_cooler_condition,
    0.0,
    0.0,
    0.0,
    0.0)
FROM
  ML.PREDICT(MODEL `osi-pi-gcp-accelerator.hydraulic.hydraulic_model_xg`,
    (
    SELECT
      *
    FROM
      `osi-pi-gcp-accelerator.hydraulic.hydraulicv`
    WHERE
      cycle_id < CAST(FORMAT_TIMESTAMP("%d%H%M",TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)) AS NUMERIC)
      AND cycle_id > CAST(FORMAT_TIMESTAMP("%d%H%M",TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 MINUTE)) AS NUMERIC))) AS model 

# Run the model and update predictions table 

MERGE
  `osi-pi-gcp-accelerator.hydraulic.hydraulic_predictions` P
USING
  (
  SELECT
    cycle AS timestamp,
    STRUCT(0.0,
      predicted_cooler_condition,
      0.0,
      0.0,
      0.0) AS prediction
  FROM
    ML.PREDICT(MODEL `osi-pi-gcp-accelerator.hydraulic.hydraulic_model_xg`,
      (
      SELECT
        *
      FROM
        `osi-pi-gcp-accelerator.hydraulic.hydraulicv`
      WHERE
        cycle_id < CAST(FORMAT_TIMESTAMP("%d%H%M",TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)) AS NUMERIC)
        AND cycle_id > CAST(FORMAT_TIMESTAMP("%d%H%M",TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 6 MINUTE)) AS NUMERIC))) AS model) T
ON
  P.timestamp = T.timestamp
  WHEN NOT MATCHED THEN INSERT (timestamp, prediction) VALUES (timestamp,prediction) 

# verify prediction results
select timestamp, prediction.Cooler from `osi-pi-gcp-accelerator.hydraulic.hydraulic_predictions`
order by timestamp desc

*/

/* copy from scheduled query
merge `osi-pi-gcp-accelerator.hydraulic.hydraulic_predictions` P using (select cycle as timestamp ,STRUCT(0.0,predicted_cooler_condition,0.0,0.0,0.0) as prediction from ML.PREDICT(MODEL `osi-pi-gcp-accelerator.hydraulic.hydraulic_model_xg`, (select * from `osi-pi-gcp-accelerator.hydraulic.hydraulicv` where cycle_id < CAST(FORMAT_TIMESTAMP("%d%H%M",TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 1 MINUTE)) as NUMERIC) and cycle_id > CAST(FORMAT_TIMESTAMP("%d%H%M",TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 20 MINUTE)) as NUMERIC))) as model) T on P.timestamp = T.timestamp when not matched then insert (timestamp,prediction) Values (timestamp,prediction)
*/
