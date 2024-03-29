# Data Scientist Salaries Analysis

## Overview

This project aims to analyze data scientist salaries using Apache Spark, a powerful open-source unified analytics engine for large-scale data processing and machine learning. We delve into a dataset containing salary information of data scientists based on various parameters like experience level, employment type, job title, and geographical location.

The goal is to provide insights into average salaries, categorize companies by size, and identify salary trends based on job titles and locations.

## Problem Statement
The dataset, ds_salaries.csv, includes details such as work year, experience level, employment type, job title, salary, and company size. The primary objectives are:

1. **Average Salary Analysis**: Calculate the average salary for each job title for employees residing in the US and Canada, ensuring the output does not contain decimal points.
2. **Enterprise Size Categorization**: Create a new field, Enterprise_size, based on the company size, categorizing companies into Large, Medium, Small, or Others.
3. **Salary Trend Analysis**: Identify jobs where the employee's residence matches the company location and the salary is greater than $50,000, then count the occurrences of each job title.

## Tools and Technologies
* **Apache Spark**: Used for data processing and analysis. Spark's in-memory computation capabilities make it ideal for handling large datasets efficiently.
* **PySpark**: The Python API for Spark, utilized for writing Spark jobs in Python.

## Solution Approach
### Reading Data
The data is read from a CSV file using PySpark's DataFrame API, which allows for efficient handling of structured data.

### Data Processing and Analysis
1. **Average Salary Analysis**: The data is filtered for employees in the US and Canada, and the average salary is calculated for each job title using the groupBy and agg functions.
2. **Enterprise Size Categorization**: A new column, Enterprise_size, is created using conditional expressions to categorize each record based on the company size.
3. **Salary Trend Analysis**: The dataset is filtered to find records where the employee's residence matches the company location and the salary is greater than $50,000. The occurrences of each job title are counted using the groupBy and count functions.

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

## Solution Overview
```mermaid
flowchart TD
    A([Start]) -->|Spark Session Initialization| B[Data Ingestion Read Data]
    B --> C[Data Processing]
    C -->|Task 1| D[Average Salary Analysis]
    C -->|Task 2| E[Enterprise Size Categorization]
    C -->|Task 3| F[Salary Trend Analysis]
    D --> |Write result_1 to CSV| G[Data Output - result_1]
    E --> |Write result_2 to CSV| H[Data Output - result_2]
    F --> |Write result_3 to CSV| I[Data Output - result_3]
    G --> J([End])
    H --> J([End])
    I --> J([End]) 
```
## High-Level Architecture
1. **Data Ingestion** : Read the ds_salaries.csv file using Spark's DataFrame API.
2. **Data Processing** :
    - Task 1: Compute the average salary by job title for employees in the US and Canada.
    - Task 2: Categorize companies based on size and add the Enterprise_size field.
    - Task 3: Analyze salary trends for job titles with specific conditions.
3. **Data Storage** : Store the processed data into CSV files for each task, ensuring the data is accessible for further analysis or reporting.

## Detailed Steps
1. **Initialize Spark Session**: Set up the Spark environment and create a Spark session to manage the application.
2. **Read Data**: Utilize PySpark to load the dataset into a Spark DataFrame.
3. **Data Analysis**:
    - Implement functions to perform the specified analyses and transformations on the data.
    - Use Spark SQL and DataFrame API for data manipulation.
3. **Write Results**: Output the results of each analysis to separate CSV files, making sure the data is neatly organized and easy to interpret.

## Data_Sc_Salaries.py explanation: 

### Libraries Explanation
* ***os***: This module provides a way to use operating system-dependent functionality like reading or writing to a filesystem. It's used here for path manipulations (e.g., os.getcwd(), os.path.isdir()).
* ***shutil***: Offers high-level operations on files and collections of files. In this context, it's used for removing the output directory (shutil.rmtree()) if it already exists to ensure a fresh start.
* ***pyspark***: The root package for Apache Spark's Python API, PySpark, which provides Spark's core functionality.
* ***from pyspark.sql import SparkSession***: SparkSession is the entry point to programming Spark with the Dataset and DataFrame API. It's used to create a Spark session.
* _from pyspark.sql.functions import *_ : Imports all SQL functions like round(), avg(), which are used for data manipulation.
* ***import pyspark.sql.functions as F***: This import statement is similar to the above but allows accessing the functions with the prefix F (e.g., F.avg()). It's not explicitly used in the provided code but is useful for namespacing and avoiding naming conflicts.
* ***from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType***: These imports are for defining schemas for DataFrames. Although the schema definition is not directly used in the provided snippets, they're essential when you need to explicitly specify the structure of your data.

### Code Explanation
* ***read_data Function***
  - Initializes reading of a CSV file into a DataFrame with headers and inferred schema. This is the first step in data processing, allowing further manipulation and analysis.

  ```python
  def read_data (spark,input_file):
      '''
      spark_session : spark
      for input_file : input_file
      '''
      # Reading the input file.
      df = spark.read.csv(input_file, header=True, inferSchema=True)
      return df
  ```
* ***load_data Function***
   - Checks if the DataFrame is not empty and writes it to a specified path as a CSV file. It uses coalesce(1) to ensure the output is a single CSV file, which is     useful for small datasets or when a consolidated file is required.

  ```python
  def load_data(data,outputpath):
      # Code to store the outputs to the respective locations      
      # The Output files are in a single partition CSV file with header.  
      if (data.count() != 0):
          print("Loading the data",outputpath)
          data.coalesce(1).write.csv(outputpath, mode="overwrite", header=True)
      else:
          print("Empty dataframe, hence cannot save the data",outputpath)
  ```
    
* ***result_1 Function***
  - Filters the input DataFrame for records where the employee resides in the US or Canada, then calculates the average salary by job title, rounds it to the         nearest whole number, and sorts by job title.

  ```python
  def result_1(input_df):
    '''
    for input file: input_df
    '''
    print("-------------------")
    print("Starting result_1")
    print("-------------------")                                        
    #  From the input_df, a new field avg_salary is created and the average salaries 
    #  given to each job for the employee residence having US and CA are calculated.                              
    # columns fetched are job_title and avg_salary
  
    df = input_df.filter(input_df.employee_residence.isin(['US', 'CA'])) \
                 .groupBy('job_title') \
                 .agg(round(avg('salary_in_usd')).alias('avg_salary')) \
                 .orderBy('job_title')
    return df
  ```
  
* ***result_2 Function***
  - Adds a new column, Enterprise_size, based on the company size with a CASE statement. It showcases how to perform conditional logic in Spark SQL.
  
  ```python
  def result_2(input_df):
    '''
    input file for this function: input_df 
    '''
    print("-------------------------")
    print("Starting result_2")
    print("-------------------------")
    # The following are the parameters : -                                                                                       
    #      ◦ input_file : input_df
    #  1) Using input_df, a new field named Enterprise_size is created.                        
    #    i) If the company size is L,                                                    
    #       the flag under Enterprise_size column is "Large_enterprise"        
    #    ii) If the company size is M,                                                   
    #       the flag under Enterprise_size column is "Medium_enterprise"       
    #   iii) If the company size is S,                                                   
    #       the flag under Enterprise_size column is "Small_enterprise"        
    #    iv) If all the above conditions are not true, flag is "others"           
    # 2) Columns fetched are: experience_level,employment_type,job_title,salary,      
    #                            company_location,company_size                                                                                                      
    mapping_expr = expr(
        """CASE WHEN company_size = 'L' THEN 'Large_enterprise'
                WHEN company_size = 'M' THEN 'Medium_enterprise'
                WHEN company_size = 'S' THEN 'Small_enterprise'
                ELSE 'others' END AS Enterprise_size""")
    df = input_df.withColumn("Enterprise_size", mapping_expr) \
                 .select('experience_level', 'employment_type', 'job_title', 'salary', 'company_location', 'company_size', 'Enterprise_size')
    return df  
  ```
  
* ***result_3 Function***
  - Filters the DataFrame for records where the employee's residence matches the company location and the salary is greater than 50,000. Then, it counts the         occurrences of each job title, showcasing filtering and aggregation.

  ```python
  def result_3(input_df):
    '''
    input file for this function: input_df 
    '''
    print("-------------------------")
    print("Starting result_3")
    print("-------------------------")                               
    # 1) Using input df, fetched the records where employee residence is matching with    
    #    the company location with the condition where salary is greater than 50000.     
    # 2) The count for each job_title is calculated                                           
    # 3) Columns fetched are: job_title,count                                                                                                           

    df = input_df.filter((input_df.employee_residence == input_df.company_location) & (input_df.salary_in_usd > 50000)) \
                 .groupBy('job_title') \
                 .count() \
                 .orderBy('job_title')
    return df 
  ```
  
* ***main Function***
  - Orchestrates the execution flow: cleaning up the output directory, creating a Spark session, reading the input data, processing it through various functions,     and finally writing the output to CSV files.

  ```python
  def main():
    """ Main driver program to control the flow of execution.
    """
    #Clean the output files for fresh execution
    outputfile_cleanup()
  
    #Get a new spark session
    spark = (SparkSession.builder
                         .appName("Data Scientist Salaries")
                         .master("local")
                         .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")

    
    cwd = os.getcwd()
    dirname = os.path.dirname(cwd)
    input_file = "file://"+ dirname + "/inputfile/ds_salaries.csv"
    output_path = "file://"+ dirname + "/output/"
    result_1_path = output_path + "result_1"
    result_2_path = output_path + "result_2"
    result_3_path = output_path + "result_3"

    task_1 = read_data(spark,input_file)
    task_2 = result_1(task_1)
    task_3 = result_2(task_1)
    task_4 = result_3(task_1)

       
    try:
        load_data(task_2,result_1_path)
    except Exception as e:
        print("Getting error while loading result_1",e)
    try:
        load_data(task_3,result_2_path)
    except Exception as e:
        print("Getting error while loading result_2",e)
    try:
        load_data(task_4,result_3_path)
    except Exception as e:
        print("Getting error while loading result_3",e)
    spark.stop()   
  ```
  
* ***outputfile_cleanup Function***
  - Ensures a clean working directory by removing the existing output directory and creating a new one. This is crucial for rerunning the script without manual         cleanup.

  ```python
  def outputfile_cleanup():
    """ Clean up the output files for a fresh execution.
        This is executed every time a job is run. 
    """
    cwd = os.getcwd()
    dirname = os.path.dirname(cwd)
    path = dirname + "/output/"
    if (os.path.isdir(path)):
        try:
            shutil.rmtree(path)  
            print("% s removed successfully" % path)
            os.mkdir(path)  
        except OSError as error:  
            print(error)  
    else:
        print("The directory does not exist. Creating..% s", path)
        os.mkdir(path)
  ```

## Drilled down flowchart 

```mermaid
graph TD
    A([Start]) --> B[Read Data]
    B --> C{Data Empty?}
    C -->|Yes| D[End]
    C -->|No| E[Task 1: Average Salary Analysis]
    E --> F[Task 2: Company Size Categorization]
    F --> G[Task 3: Salary Trend Analysis]
    G --> H{Data Processed Correctly?}
    H -->|No| I[Log Error and End]
    H -->|Yes| J[Write Data to CSV]
    J --> K([End])

    subgraph Task 1: Average Salary Analysis
    E --> E1[Filter by Employee Residence US, CA]
    E1 --> E2[Group by Job Title]
    E2 --> E3[Calculate and Round Average Salary]
    E3 --> E4[Return DataFrame]
    end

    subgraph Task 2: Company Size Categorization
    F --> F1[Add Enterprise_size Column]
    F1 --> F2[Map Company Size to Categories]
    F2 --> F3[Select Required Columns]
    F3 --> F4[Return DataFrame]
    end

    subgraph Task 3: Salary Trend Analysis
    G --> G1[Filter by Employee Residence and Salary > 50,000]
    G1 --> G2[Group by Job Title and Count]
    G2 --> G3[Order by Job Title]
    G3 --> G4[Return DataFrame]
    end
```

## CRC Files Explanation in the output folder
When Spark writes data to a file system, it also creates CRC (Cyclic Redundancy Check) files alongside the actual data files. These CRC files, such as ._SUCCESS.crc and .part-00000-...csv.crc, are checksum files used to detect errors in the written data files. They help in ensuring data integrity by allowing Spark (or the underlying file system) to verify that the data has not been corrupted during the write process.

* ***._SUCCESS.crc***: A checksum file for the _SUCCESS file, which Spark creates in the output directory to indicate that the data was written successfully.
* ***.part-00000-...csv.crc***: Corresponds to the checksum of the actual data part file written by Spark. Each part file will have its associated CRC file.
These files are not needed for data analysis but are crucial for maintaining data integrity, especially in distributed computing environments where data is written and read across multiple nodes.

## Conclusion
This project showcases the power of Apache Spark in processing and analyzing large datasets efficiently. Through this challenge, we demonstrate how to perform data aggregation, filtering, and transformation operations to extract meaningful insights from salary data.

