# -*- coding: utf-8 -*-
import os
import shutil
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import pyspark.sql.functions as F 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType


def read_data (spark,input_file):
    '''
    spark_session : spark
    for input_file : input_file
    '''
    #  Reading the input file.
    df = spark.read.csv(input_file, header=True, inferSchema=True)

    return df

def load_data(data,outputpath):
     # Stores the outputs to the respective locations.      
     # The Output files are a single partition CSV file with header.  
     if (data.count() != 0):
        print("Loading the data",outputpath)
        data.coalesce(1).write.csv(outputpath, mode="overwrite", header=True)
     else:
        print("Empty dataframe, hence cannot save the data",outputpath)
def result_1(input_df):
    '''
    for input file: input_df
    '''
    print("-------------------")
    print("Starting result_1")
    print("-------------------")
    #  From the input_df, a new field avg_salary is created and the average salaries 
    #  given to each job for the employee residence having US and CA are calculated. 
    #  Columns fetched are: job_title, avg_salary 
    df = input_df.filter(input_df.employee_residence.isin(['US', 'CA'])) \
                 .groupBy('job_title') \
                 .agg(round(avg('salary_in_usd')).alias('avg_salary')) \
                 .orderBy('job_title')

    return df

def result_2(input_df):
    '''
    input file for this function: input_df 
    '''
    print("-------------------------")
    print("Starting result_2")
    print("-------------------------")

    # The following are the parameters : -                                                                                       
    #      ◦ input_file : input_df                                                                                                                                                                                                                       |
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
    # 3) Columns fetched are: job_title,count                                                                                                                       |

    df = input_df.filter((input_df.employee_residence == input_df.company_location) & (input_df.salary_in_usd > 50000)) \
                 .groupBy('job_title') \
                 .count() \
                 .orderBy('job_title')
    return df                
     


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


if __name__ == "__main__":
	main()
