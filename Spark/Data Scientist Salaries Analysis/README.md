# Data Scientist Salaries Analysis

## Overview

This project aims to analyze data scientist salaries using Apache Spark, a powerful open-source unified analytics engine for large-scale data processing and machine learning. We delve into a dataset containing salary information of data scientists based on various parameters like experience level, employment type, job title, and geographical location.

The goal is to provide insights into average salaries, categorize companies by size, and identify salary trends based on job titles and locations.

## Problem Statement
The dataset, ds_salaries.csv, includes details such as work year, experience level, employment type, job title, salary, and company size. The primary objectives are:

1. **Average Salary Calculation**: Calculate the average salary for each job title for employees residing in the US and Canada, ensuring the output does not contain decimal points.
2. **Enterprise Size Categorization**: Create a new field, Enterprise_size, based on the company size, categorizing companies into Large, Medium, Small, or Others.
3. **High Salary Job Count**: Identify jobs where the employee's residence matches the company location and the salary is greater than $50,000, then count the occurrences of each job title.

## Tools and Technologies
* **Apache Spark**: Used for data processing and analysis. Spark's in-memory computation capabilities make it ideal for handling large datasets efficiently.
* **PySpark**: The Python API for Spark, utilized for writing Spark jobs in Python.

## Solution Approach
### Reading Data
The data is read from a CSV file using PySpark's DataFrame API, which allows for efficient handling of structured data.

### Data Processing and Analysis
1. **Average Salary Calculation**: The data is filtered for employees in the US and Canada, and the average salary is calculated for each job title using the groupBy and agg functions.
2. **Enterprise Size Categorization**: A new column, Enterprise_size, is created using conditional expressions to categorize each record based on the company size.
3. **High Salary Job Count**: The dataset is filtered to find records where the employee's residence matches the company location and the salary is greater than $50,000. The occurrences of each job title are counted using the groupBy and count functions.

### Writing Results
The results of each analysis are written back to CSV files, ensuring that the output is easily accessible and interpretable.

## How to Run
1. Ensure Apache Spark and PySpark are installed and configured on your system.
2. Open a terminal and navigate to the project directory.
3. Run the Spark job using the command:

```shell
spark-submit Data_Sc_Salaries.py
```

4. Check the output directories for the results.


```mermaid
graph TD;
    Start-->Data Ingestion (Read Data);
    Data Ingestion (Read Data)-->Data Processing (Average Salary Analysis, Company Size Categorization, Salary Trend Analysis;
    Data Processing (Average Salary Analysis, Company Size Categorization, Salary Trend Analysis-->Data Output (Write Data);
    Data Output (Write Data)-->End;
```

## Conclusion
This project showcases the power of Apache Spark in processing and analyzing large datasets efficiently. Through this challenge, we demonstrate how to perform data aggregation, filtering, and transformation operations to extract meaningful insights from salary data.

### For Beginners
If you're new to Apache Spark or data engineering, this project is a great starting point to understand how big data technologies work in practice. The code is structured and commented to make it as understandable as possible. Feel free to explore the scripts and modify them to get a hands-on experience with Spark.
