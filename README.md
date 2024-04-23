### Resource and Background :
Retrieve the math performance dataset from https://archive.ics.uci.edu/dataset/320/student+performance. The metadata is available in the 'student.txt' file.

Let's suppose the math performance dataset is obtained through an automated evaluation system. This data arrives on a scheduled basis (e.g., daily, weekly, monthly) and requires processing to generate student-based metrics. The objective is to read the dataset and populate the data into an RDBMS. The choice to insert the data into an open-source RDBMS is optional.

## Solution :
![Screenshot 2024-04-23 160755](https://github.com/amyth-singh/justplay-infra-pipeline-development/assets/78929302/083de1d8-8662-42f7-a80c-9aa01cbf4636)

```Assuming that the incoming data is provided in CSV format, the solution is constructed to manage various methods of data input, including manual uploads, bulk uploads, and scripted extractions or ingestions (such as through a pipeline). It is designed to emulate an event-driven architecture model when deployed locally, and when transitioned to any cloud platform, it allows for serverless trigger capabilities and automatic scaling features.```

```The current solution in the repository oversees a designated folder where CSV files are expected to be placed or introduced. Upon the arrival of CSV files in this folder, it initiates an automated process involving extraction, validation, and pre-processing. This process includes removing rows with missing or null values, standardizing values and field names to lowercase, appending an auto-incremental 'id' column, adding a 'creating_timestamp' column for incremental dataset construction, and substituting the delimiter with a comma. Additionally, the system individually assesses each CSV file against the schema defined in the 'schema.yaml' file to ensure compliance.```

```Once the pre-processing and validation stages are completed without issues, the compliant CSV file proceeds into a rule-based system. In this system, if the file meets the specified criteria, it undergoes conversion and compression into a Parquet file, after which it is relocated to a designated folder. Conversely, if the CSV file fails validation or pre-processing, it is transferred to a designated "failed" folder. This allows analysts or engineers to manually review the problematic files for further investigation.```

``` Subsequently, the successful Parquet files within the folder initiate an automated pipeline responsible for uploading the Parquet file to a PostgresSQL/MySQL database. In an optimal scenario, this pipeline would additionally transfer the files to a data warehouse, enabling the analytics team to access and analyze the data. Furthermore, it would move the files to an object store, facilitating widespread access for other technical teams. However, for the purposes of the project, the files are uploaded to a local database to facilitate viewing and execution of SQL queries.```

### Answering Requirements :
1. The solution should be easy to reproduce and automate across all stages: data collection, preparation, modeling, and presentation.
```During the workflow of the solution, every step is meticulously documented and recorded in a 'conversion_log.txt' file, facilitating documentation and debugging. Furthermore, each stage is designed to be accessible and reproducible, enabling seamless replication across various environments. Additionally, the utilisation of trigger-based and rule-based integrations ensures automation, consistency, and reliability throughout the entirety of the solution's lifecycle.```

2. It should handle potential data quality issues like missing data.
```Presently, the solution addresses basic data quality issues including missing values, null values, schema mismatches, and incorrect delimiters. However, there is potential for the solution to evolve and tackle more complex challenges such as data type inconsistencies, data formatting errors, duplicate entries, incomplete datasets, outlier and anomaly detection, and data loss prevention over time. To maintain efficiency and simplicity, the solution currently focuses on managing fundamental data quality concerns. ```

3. The solution should follow good data management practices, ensuring accessibility for various user profiles (e.g., scientists, business stakeholders).
```The pipeline currently checks, compresses and stores valid CSVs into a folder. In an ideal scenario, this folder would be a cloud object store where access control, data security, data retention and other lifecycle management processes can be ensured. At the moment the solution incorporates some aspects of good data management practices like data validation, logging, automation, schema management, error handling and data disposal ```

4. Provide a way to serve and visualise the data. 
5. Dashboards and/or plots should be runnable on open-source software, both locally and on the system.

### Alternative Scenarios :
What could be done if data volume increases 100x?
```Each original CSV file measures 56 KB. Following compression and conversion, each resulting Parquet file is reduced to 17 KB, representing a compression rate of approximately 69.64%. The entire conversion process of 999 CSV files concluded within 10.90 seconds, equating to an average conversion time of 0.01091 seconds per file. Scaling up by 100x would involve processing 99,900 files, requiring an estimated duration of 1088.109 seconds or approximately 18.13515 minutes. To ensure the system maintains efficiency and reliability as it scales, optimising data processing for parallelisation and distributed computing is paramount. Implementing several other measures such as elastic scaling capabilities, multi-node fault-tolerant storage, automated resource allocation, serverless deployment of the solution can help handle data volume, and finally, having more robust data quality and validation rules.```

What could be done if data is delivered frequently at 6am every two days?
```The solution is crafted with event streaming as its core focus. Deploying it onto a function-as-a-service platform like 'Google Cloud Functions' or 'AWS Glue' enables responsiveness to inbound or manual data drops of varying frequencies, recency, and volumes. If cloud deployment is not viable, operating the solution locally via a 'cronjob' facilitates a recurring scheduling mechanism as well.```

What could be done if the data has to be made available to a bigger organisation of 1000+ people?
```As scalability becomes a priority, transitioning the solution to robust cloud services such as Google DataProc, Google DataFlow, or similar big data processing platforms becomes essential. These services leverage frameworks like Apache Spark, Apache Beam, or Apache Flink to handle large-scale data processing efficiently. Additionally, employing scalable database services such as Amazon RDS or Google Cloud SQL becomes necessary to support a larger user base. Establishing access control, governance, compliance policies, performance monitoring, and self-service and data visualisation solutions should also be considered to ensure the smooth operation and management of the system.```

# Part 2
Use the data in the RDBMS from part 1 and write SQL quries to answer the following :

1. List of unique “mother’s job” for male students younger than 20 years old.
2. Most frequent “travel time” among students that live in rural areas
3. Top 3 “father’s job” for students grouped by parent’s cohabitation status.
4. Most frequent “class failures” label grouped by family sizes.
5. Median “absences” for average and low family relationship qualities, group by sex.

# Part 3
_note - this is a design exercise, no code implementation is needed._
You will be responsible for designing and implementing a data ingestion pipeline for telemetry data generated by the mobile games. This data includes information such as player actions, level progression, and in-app purchases.
The goal of this exercise is to demonstrate your ability to design and implement a scalable, reliable, and maintainable data ingestion pipeline using modern big data technologies, along with a data visualisation layer for stakeholders.

### Requirements :
Provide schematics, explanations and reasoning for the above use-cases.
