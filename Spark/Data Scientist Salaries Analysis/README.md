Spark Data Engineering Challenge: Data Scientist Salaries Analysis
Overview
This repository contains the solution to a data engineering challenge focused on analyzing data scientist salaries using Apache Spark. The challenge involves reading, processing, and analyzing salary data to extract meaningful insights through various data transformations and aggregations.

Apache Spark is an open-source, distributed computing system that provides an interface for programming entire clusters with implicit data parallelism and fault tolerance. It's widely used for big data processing and analytics.

Problem Statement
The challenge is based on a dataset containing salary information for data scientists across different countries, experience levels, and company sizes. The primary objectives are:

Average Salary Calculation: Calculate the average salary for each job title for employees residing in the US and Canada, ensuring the output does not contain decimal points.
Enterprise Size Categorization: Based on the company size, categorize each record into Large_enterprise, Medium_enterprise, Small_enterprise, or others.
High Salary Job Count: Identify jobs where the employee's residence matches the company location and the salary is greater than $50,000, then count the occurrences of each job title.
Tools and Technologies
Apache Spark: Used for data processing and analysis.
PySpark: The Python API for Spark, utilized for writing Spark jobs in Python.
Visual Studio Code (VSCode): Recommended as the Integrated Development Environment (IDE) for writing and testing the Spark code.
Solution Approach
Reading Data
The data is read from a CSV file using PySpark's DataFrame API, which allows for efficient handling of structured data.

Data Processing and Analysis
Average Salary Calculation: The data is filtered for employees in the US and Canada, and the average salary is calculated for each job title using the groupBy and agg functions.
Enterprise Size Categorization: A new column, Enterprise_size, is created using conditional expressions to categorize each record based on the company size.
High Salary Job Count: The dataset is filtered to find records where the employee's residence matches the company location and the salary is greater than $50,000. The occurrences of each job title are counted using the groupBy and count functions.
Writing Results
The results of each analysis are written back to CSV files, ensuring that the output is easily accessible and interpretable.

How to Run
Ensure Apache Spark and PySpark are installed and configured on your system.
Open a terminal and navigate to the project directory.
Run the Spark job using the command:
php
Copy code
spark-submit <script_name>.py
Check the output directories for the results.
Conclusion
This project showcases the power of Apache Spark in processing and analyzing large datasets efficiently. Through this challenge, we demonstrate how to perform data aggregation, filtering, and transformation operations to extract meaningful insights from salary data.

For Beginners
If you're new to Apache Spark or data engineering, this project is a great starting point to understand how big data technologies work in practice. The code is structured and commented to make it as understandable as possible. Feel free to explore the scripts and modify them to get a hands-on experience with Spark.
