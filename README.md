# Part 1
### Resource :
Retrieve the math performance dataset from https://archive.ics.uci.edu/dataset/320/student+performance. The metadata is available in the 'student.txt' file.

## Background - Code Implementation :
Let's suppose the math performance dataset is obtained through an automated evaluation system. This data arrives on a scheduled basis (e.g., daily, weekly, monthly) and requires processing to generate student-based metrics. The objective is to read the dataset and populate the data into an RDBMS. The choice to insert the data into an open-source RDBMS is optional.

### Requirements :
1. The solution should be easy to reproduce and automate across all stages: data collection, preparation, modeling, and presentation.
2. It should handle potential data quality issues like missing data.
3. The solution should follow good data management practices, ensuring accessibility for various user profiles (e.g., scientists, business stakeholders).
4. Provide a way to serve and visualise the data. 
5. Dashboards and/or plots should be runnable on open-source software, both locally and on the system.

### Alternative Scenarios :
What could be done if data volume increases 100x?
What could be done if data is delivered frequently at 6am every two days?
What could be done if the data has to be made available to a bigger organisation of 1000+ people?

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
