# Part 1 - Pipeline

#### Resource and Background :
Retrieve the math performance dataset from [DataSet](https://archive.ics.uci.edu/dataset/320/student+performance). The metadata is available in the 'student.txt' file.

Let's suppose the math performance dataset is obtained through an automated evaluation system. This data arrives on a scheduled basis (e.g., daily, weekly, monthly) and requires processing to generate student-based metrics. The objective is to read the dataset and populate the data into an RDBMS. The choice to insert the data into an open-source RDBMS is optional.

## Solution :
![Screenshot 2024-04-23 164807](https://github.com/amyth-singh/justplay-infra-pipeline-development/assets/78929302/c29b0023-11fb-48e8-9b4c-a69591ad16c3)

Given incoming data in CSV format, the solution manages multiple input methods such as manual uploads, bulk uploads, or scripted extractions via pipelines. It's built to mimic a local event-driven architecture but is adaptable to any cloud platform, allowing for serverless, trigger-based, and automatic scaling capabilities. Within the repository, it supervises an ```input_csv``` folder, where CSV files are expected. Upon their arrival, it initiates an automated process involving extraction, validation, and pre-processing, including removing null rows, standardizing values, and field names, and changing delimiters. Each CSV file is individually checked against the schema defined in ```schema.yaml``` for compliance. Validated files enter a rule-based system: _compliant_ ones are converted to Parquet and moved to ```output_parquet```, while _failures_ go to ```output_failed``` within input_csv for manual review. Successful Parquet files trigger an automated pipeline to upload data to a ```MySQL``` database, using credentials from ```config.yaml``` and schema from ```schema_sql.yaml```. Ideally, this pipeline would extend to a data warehouse like BigQuery for analytics and to an object store such as Google Cloud Storage or S3 for broader access. However, for project purposes, files are uploaded to a local database for easier viewing and SQL query execution.

> [!NOTE]
> View ```conversion_log.txt``` log details.

### Instructions on How-To-Use:	
<p align="center">
<img src="https://github.com/amyth-singh/justplay-infra-pipeline-development/assets/78929302/06bbb073-2305-4c88-8ee0-97bed201eddd" alt="main.py" style="width:600px;"/>
</p>
<br>

```main.py``` - This is the main python script where all the functionality, methords and features are housed. To use this file,first start with replacing fields that have ```student_data``` with the table name you desire. Note, create the table in your MySQL database to make the script run smoothly. Second, install all the modules necesssary to run the main.py script. 

Here's the list : 
```python
import pandas as pd
import yaml
import time
import os
import logging
import mysql.connector
from datetime import datetime
from watchdog.observers import Observer
from watchdog.events import FileSystemEventHandler
from sqlalchemy import create_engine, text
```

```input_csv``` - This folder is monitored for CSV files. Drop bulk or individual CSVs here to trigger the pipeline.

```output_failed``` - This folder should be located inside the ```input_csv``` folder

## Answering Requirements :
1. The solution should be easy to reproduce and automate across all stages: data collection, preparation, modeling, and presentation.

<table><tr><td>

During the workflow of the solution, every step is meticulously documented and recorded in a ```conversion_log.txt``` file, facilitating documentation and debugging. Furthermore, each stage is designed to be accessible and reproducible, enabling seamless replication across various environments. Additionally, the utilisation of trigger-based and rule-based integrations ensures automation, consistency, and reliability throughout the entirety of the solution's lifecycle.
</td></tr></table>

2. It should handle potential data quality issues like missing data.

<table><tr><td>
Presently, the solution addresses basic data quality issues including missing values, null values, schema mismatches, and incorrect delimiters. However, there is potential for the solution to evolve and tackle more complex challenges such as data type inconsistencies, data formatting errors, duplicate entries, incomplete datasets, outlier and anomaly detection, and data loss prevention over time. To maintain efficiency and simplicity, the solution currently focuses on managing fundamental data quality concerns.
</td></tr></table>

3. The solution should follow good data management practices, ensuring accessibility for various user profiles (e.g., scientists, business stakeholders).

<table><tr><td>
The pipeline currently checks, compresses and stores valid CSVs into a folder. In an ideal scenario, this folder would be a cloud object store where access control, data security, data retention and other lifecycle management processes can be ensured. At the moment the solution incorporates some aspects of good data management practices like data validation, logging, automation, schema management, error handling and data disposal.
</td></tr></table>
  
4. Provide a way to serve and visualise the data. 
5. Dashboards and/or plots should be runnable on open-source software, both locally and on the system.

## Alternative Scenarios :
What could be done if data volume increases 100x?

<table><tr><td>
Each original CSV file measures 56 KB. Following compression and conversion, each resulting Parquet file is reduced to 17 KB, representing a compression rate of approximately 69.64%. The entire conversion process of 999 CSV files concluded within 10.90 seconds, equating to an average conversion time of 0.01091 seconds per file. Scaling up by 100x would involve processing 99,900 files, requiring an estimated duration of 1088.109 seconds or approximately 18.13515 minutes. To ensure the system maintains efficiency and reliability as it scales, optimising data processing for parallelisation and distributed computing is paramount. Implementing several other measures such as elastic scaling capabilities, multi-node fault-tolerant storage, automated resource allocation, serverless deployment of the solution can help handle data volume, and finally, having more robust data quality and validation rules.
</td></tr></table>

What could be done if data is delivered frequently at 6am every two days?

<table><tr><td>
The solution is crafted with event streaming as its core focus. Deploying it onto a function-as-a-service platform like 'Google Cloud Functions' or 'AWS Glue' enables responsiveness to inbound or manual data drops of varying frequencies, recency, and volumes. If cloud deployment is not viable, operating the solution locally via a 'cronjob' facilitates a recurring scheduling mechanism as well.
</td></tr></table>

What could be done if the data has to be made available to a bigger organisation of 1000+ people?

<table><tr><td>
As scalability becomes a priority, transitioning the solution to robust cloud services such as Google DataProc, Google DataFlow, or similar big data processing platforms becomes essential. These services leverage frameworks like Apache Spark, Apache Beam, or Apache Flink to handle large-scale data processing efficiently. Additionally, employing scalable database services such as Amazon RDS or Google Cloud SQL becomes necessary to support a larger user base. Establishing access control, governance, compliance policies, performance monitoring, and self-service and data visualisation solutions should also be considered to ensure the smooth operation and management of the system.
</td></tr></table>

# Part 2 - SQL
Use the data in the RDBMS from part 1 and write SQL quries to answer the following :

1. List of unique “mother’s job” for male students younger than 20 years old.

```mysql
SELECT DISTINCT mjob
FROM just_play_db.student_data
WHERE sex = 'm' AND age < 20 AND mjob IS NOT NULL;
```
|mjob    |
|--------|
|services|
|other   |
|health  |
|teacher |
|at_home |

2. Most frequent “travel time” among students that live in rural areas

```mysql
SELECT traveltime, COUNT(*) AS count
FROM just_play_db.student_data
WHERE address = 'R'
GROUP BY traveltime
ORDER BY count DESC
LIMIT 1;
```
|traveltime|count|
|----------|-----|
|1         |35   |

3. Top 3 “father’s job” for students grouped by parent’s cohabitation status.
   
```mysql
SELECT s.pstatus, s.fjob, s.job_count
FROM (
    SELECT pstatus, fjob, job_count,
           ROW_NUMBER() OVER (PARTITION BY pstatus ORDER BY job_count DESC) AS rn
    FROM (
        SELECT pstatus, fjob, COUNT(*) AS job_count
        FROM just_play_db.student_data
        GROUP BY pstatus, fjob
    ) AS counted_jobs
) AS s
WHERE s.rn <= 3;
```
|pstatus|fjob      |job_count|
|-------|----------|---------|
|a      |other     |23       |
|a      |services  |7        |
|a      |teacher   |5        |
|t      |other     |194      |
|t      |services  |104      |
|t      |teacher   |24       |

4. Most frequent “class failures” label grouped by family sizes.
   
```mysql
SELECT s.famsize, s.failures, s.class_fail_count
FROM (
	SELECT famsize, failures, class_fail_count,
		ROW_NUMBER() OVER (PARTITION by famsize ORDER BY class_fail_count DESC) AS rn
	FROM (
		SELECT famsize, failures, COUNT(*) AS class_fail_count
        FROM just_play_db.student_data
        GROUP BY famsize, failures
    ) AS counted_count
) AS s
WHERE rn = 1;
```
|famsize  |failures  |class_fail_freq|
|---------|----------|---------------|
|gt3      |0         |222            |
|le3      |0         |90             |

5. Median “absences” for average and low family relationship qualities, group by sex.

```The query provided calculates the median "absences" for individuals with average and low family relationship qualities, grouped by sex. It ensures that the median calculation is accurate by considering both odd and even counts of absences within each group.```

```mysql
SELECT sex, famrel, 
    CASE
        WHEN COUNT(*) % 2 = 1 THEN AVG(absences)
        ELSE (SUM(absences) / 2)
    END AS median_absences
FROM (
    SELECT sex, famrel, absences,
        @rn := IF(@prev_sex = sex AND @prev_famrel = famrel, @rn + 1, 1) AS rn,
        @prev_sex := sex,
        @prev_famrel := famrel
    FROM 
        (SELECT *, (@rn := 0) FROM just_play_db.student_data WHERE famrel <= 3 ORDER BY sex, famrel, absences) sorted,
        (SELECT @prev_sex := NULL, @prev_famrel := NULL) init
) ranked
WHERE 
    rn = CEIL((SELECT COUNT(*) FROM just_play_db.student_data WHERE famrel <= 3 AND sex = ranked.sex AND famrel = ranked.famrel) / 2)
    OR rn = FLOOR((SELECT COUNT(*) FROM just_play_db.student_data WHERE famrel <= 3 AND sex = ranked.sex AND famrel = ranked.famrel) / 2) + 1
GROUP BY sex, famrel
ORDER BY sex, famrel;
```
|sex|famreal|median_absences|
|---|-------|---------------|
|f  |1	    |14             |
|f  |2	    |15             |
|f  |3	    |2              |
|m  |1	    |5              |
|m  |2	    |3              |
|m  |3	    |2              |

# Part 3 - Infrastructure
_note - this is a design exercise, no code implementation is needed._
You will be responsible for designing and implementing a data ingestion pipeline for telemetry data generated by the mobile games. This data includes information such as player actions, level progression, and in-app purchases.
The goal of this exercise is to demonstrate your ability to design and implement a scalable, reliable, and maintainable data ingestion pipeline using modern big data technologies, along with a data visualisation layer for stakeholders.

## Requirements :
Provide schematics, explanations and reasoning for the above use-cases.
