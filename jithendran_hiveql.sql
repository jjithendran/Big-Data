-- Databricks notebook source
-- MAGIC %md
-- MAGIC Hive Query Scripts

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Create clinicaltrial_2021 Table in HIVE

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS clinicaltrial_2021(  Id STRING, Sponsor STRING,
Status STRING, Start STRING, Completion STRING, 
Type String, Submission STRING, Conditions STRING, Interventions STRING) 
ROW FORMAT DELIMITED FIELDS TERMINATED BY '|' 
LINES TERMINATED BY '\n'   
LOCATION 'FileStore/tables/clinicaltrial_2021';



-- COMMAND ----------

SELECT * FROM clinicaltrial_2021 LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Pharma Table 

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS pharma(Company STRING, 
Parent_Company STRING, 
Penalty_Amount STRING, 
Subtraction_From_Penalty STRING,
Penalty_Amount_Adjusted_For_Eliminating_Multiple_Counting STRING, 
Penalty_Year STRING, 
Penalty_Date STRING, 
Offense_Group STRING,
Primary_Offense STRING, 
Secondary_Offense STRING, 
Description STRING, 
Level_of_Government STRING, 
Action_Type STRING, 
Agency STRING,
Civil_Criminal STRING, 
Prosecution_Agreement STRING, 
Court STRING, 
Case_ID STRING, 
Private_Litigation_Case_Title STRING, 
Lawsuit_Resolution STRING, 
Facility_State STRING,
City STRING, 
Address STRING, 
Zip STRING, 
NAICS_Code STRING, 
NAICS_Translation STRING, 
HQ_Country_of_Parent STRING,
HQ_State_of_Parent STRING, 
Ownership_Structure STRING, 
Parent_Company_Stock_Ticker STRING,
Major_Industry_of_Parent STRING, 
Specific_Industry_of_Parent STRING, 
Info_Source STRING, 
Notes STRING)
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' 
LINES TERMINATED BY '\n' 
LOCATION 'FileStore/tables/pharma';

-- COMMAND ----------

SELECT * FROM pharma LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Creating Mesh Table

-- COMMAND ----------

CREATE TABLE IF NOT EXISTS mesh( term STRING, tree STRING) 
ROW FORMAT DELIMITED 
FIELDS TERMINATED BY ','  
LINES TERMINATED BY '\n'   
LOCATION 'FileStore/tables/mesh';

-- COMMAND ----------

SELECT * FROM mesh LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q1. Total Case Studies in 2021 

-- COMMAND ----------

SELECT COUNT(DISTINCT Id) FROM clinicaltrial_2021;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q2. Most Frequent Type of Studies

-- COMMAND ----------

SELECT Type, COUNT(Type) as Count FROM clinicaltrial_2021  GROUP BY Type ORDER BY Count DESC;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q3. Most Frequent Conditions

-- COMMAND ----------

SELECT Condition, COUNT(Condition) as Count FROM 
(SELECT EXPLODE(SPLIT(Conditions, ",")) as Condition 
FROM clinicaltrial_2021) as Condition GROUP BY Condition  
HAVING Condition != ""  ORDER BY Count DESC LIMIT 5;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q4. Most Common Root word According to Conditions

-- COMMAND ----------

SELECT tree.root, COUNT(tree.root) as Count FROM  (SELECT EXPLODE(SPLIT(Conditions, ",")) as Condition 
FROM clinicaltrial_2021) as Condition LEFT JOIN (SELECT term, SPLIT(tree, "\\.")[0] as root FROM mesh) 
as tree ON   (Condition.Condition = tree.term) GROUP BY tree.root  ORDER BY Count DESC LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q5.Top Sponsors which are not pharmaceutical companies

-- COMMAND ----------

SELECT trial.Sponsor, COUNT(trial.Sponsor) as Count 
FROM clinicaltrial_2021 AS trial
LEFT OUTER JOIN pharma 
ON trial.Sponsor = pharma.Parent_Company  
GROUP BY trial.Sponsor, pharma.Parent_Company 
HAVING pharma.Parent_Company IS NULL 
ORDER BY Count 
DESC 
LIMIT 10;

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Q6. Number of Studies completed month wise in 2021

-- COMMAND ----------


SELECT month, COUNT(month) as count FROM 
(SELECT SPLIT(Completion, " ")[0] as month, SPLIT(Completion, " ")[1] as year, 
Status FROM clinicaltrial_2021) as temp 
WHERE  temp.year = "2021" AND Status = "Completed" 
GROUP BY month  ORDER BY count desc ;

-- COMMAND ----------


