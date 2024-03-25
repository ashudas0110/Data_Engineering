import os
import shutil
import pyspark
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.functions import * 
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, FloatType, DateType


def read_data (spark,input_file):
    ''' 
    spark_session : spark 
    for input_file : input_file 
    '''

#The following are the parameters : -						//
#spark session : spark								//
#input_file : input_path							//
# 1.Loaded the CSV file into a dataframe.						//
# 2.Dropped the rows if it’s having null values in any of the given columns.	//

    df = spark.ad.csv(input_file,header=True,schema=schema).na.drop()
    return df  

def load_data(data,output_path):

#------------------------------------------------------------------------
# 1. Stores the outputs to the respective locations. 	|
# Note:									|
# • Output files are a single partition CSV file with header.	|
# • load_data function is important for all the tasks. 			|
#------------------------------------------------------------------------

    if (data.count() != 0):
        print("Loading the data",output_path)
        data.coalesce(1).write.csv(output_path, header=True, mode='overwrite')

    else:
        print("Empty dataframe, hence cannot save the data",output_path)


def result_1(input_df):
    '''
    for input file: input_file
    '''
    print("-------------------")
    print("Starting result_1")
    print("-------------------")


#--------------------------------------------------------------------------------------------------------------------------------------------------------
# result_1																		
# The following operations are completed inside the result_1 function:										
# The following are the parameters : -																
#      ◦ input_file : input_file
# input_df is used to complete the task																
#  1.a column 'Percentage' is created 
#  2.In that column, the percentage of each student is found and the value is round to the nearest integer.
#  3.Then order the 'Percentage' column by percentage in descending order and education type in ascending order.
#  4.Now, create a column 'Grade'.
#  5.In that column you are going to perform the below given operations:
#     - If the percentage is greater than or equal to 91 then Grade should be “A+”.
#     - If the percentage is greater than or equal to 81 and less than 91 then Grade should be “A”.
#     - If the percentage is greater than or equal to 71 and less than 81 then Grade should be “B+”.
#     - If the percentage is greater than or equal to 61 and less than 71 then Grade should be “B”.
#     - If the percentage is greater than or equal to 51 and less than 61 then Grade should be “C+”.
#     - If the percentage is greater than or equal to 41 and less than 51 then Grade should be “C”.
#     - If the percentage is greater than or equal to 35 and less than 41 then Grade should be “D”.
#     - Otherwise Grade should be “E”.
#  6.Create a column as 'Result'.
#  7.In that column you are going to perform the below given operations:
#     - If the grade is in “A+” , ”A” , ”B+” , ”B” , ”C+” , “C” , ”D” then Result should be “PASS”.
#     - Otherwise Result should be “FAIL”.


    # Calculate the percentage and round it to the nearest integer
    df = input_df.withColumn("Percentage", ceil((col("math_score")+col("reading_score")+col("writing_score"))/3))

    # Order by Percentage descending and education_type ascending
    df= df.orderBy(df.Percentage.desc(), df.education_type.asc())

    # Assign grades
    grade_expr= when (col("Percentage")>=91, "A+") \ 
            .when((col("Percentage") >=81 ) & (col("Percentage") < 91), "A") \
            .when((col("Percentage") >= 71) & (col("Percentage") < 81), "B+") \
            .when((col("Percentage") >= 61) & (col("Percentage") < 71), "B") \
            .when((col("Percentage") >= 51) & (col("Percentage") < 61), "C+") \
            .when((col("Percentage") >= 41) & (col("Percentage") < 51), "C") \
            .when((col("Percentage") >= 35) & (col("Percentage") < 41), "D") \
            .otherwise("E")

    df=df.withColumn("Grade", grade_expr) 

    # Assign result based on grade
    result_expr= when(col("Grade").isin(["A+", "A", "B+", "B", "C+", "C", "D"]), "PASS")\
                .otherwise("FAIL")
    df=df.withColumn("Result", result_expr)

    return df.select("name", "education_type", "Percentage", "Grade", "Result")


def result_2(input_df):
    '''
    for input file: input_df
    '''
    print("-------------------------")
    print("Starting result_2")
    print("-------------------------")

# The following are the parameters : -								|
#      ◦ input_file : input_df									|
# Use input_df to complete the task								|
#  1.Create column as 'Percentage'
#  2.Find the percentage of each student and the percentage should be round to 2 decimal values.
#  3.Then order by the Percentage column in descending order and fetch the top 5 values.
#  4.Create a column as 'Rank'
#  5.Give the rank based on the percentage 

    # Calculate the percentage to two decimal places
    df = input_df.withColumn("Percentage", round((col("math_score")+col("reading_score")+col("writing_score"))/3,2))

    # Order by Percentage descending
    df=df.orderBy(df.Percentage.desc())

    # Assign rank
    windowSpec= Window.orderBy(df.Percentage.desc())
    df = df.withColumn("Rank", rank().over(windowSpec))
  
    # Fetch top 5
    df = df.limit(5)
    
    return df.select("name", "education_type", "Percentage", "Rank")

def result_3(input_df):
    '''
    for input file: input_df
    '''
    print("-------------------------")
    print("Starting result_3")
    print("-------------------------")

# The following are the parameters : -								|
#      ◦ input_file : input_df									|
#  Use the input_df to complete the task							|
#  1.Create a column 'Percentage'
#  2.Find the percentage of each student and round the percentage to 2 decimal values.
#  3.Now,get the students who has greater than or equal to 35. i.e. PASS
#  4.Now,Create a column 'Pass_Percentage'.
#  5.Now,Find the pass percentage of each assignment status based on the percentage,
#  6.Then round the Pass_Percentage to 2 decimal values.

    # Calculate the percentage to two decimal places
    df = input_df.withColumn("Percentage", round((col("math_score")+col("reading_score")+col("writing_score"))/3,2)) 
    
    # Filter students who passed
    df_pass = df.filter(col("Percentage")>=35)
    
    # Calculate pass percentage for each assignment status
    df_result = df_pass.groupBy("assignment_status") \
            .agg(round((count("name") / df_pass.count()) * 100, 2).alias("Pass_Percentage"))
                                      
    return df_result.select("assignment_status", "Pass_Percentage")   


def main():

    """ Main driver program to control the flow of execution.
    """
    #Clean the output files for fresh execution
    outputfile_cleanup()
    #Get a new spark session
    spark = (SparkSession.builder
                         .appName("StudentsPerformance")
                         .master("local")
                         .getOrCreate())
    spark.sparkContext.setLogLevel("ERROR")
    
    global schema
    schema = StructType([ \
    StructField("name",StringType(),True), \
    StructField("gender",StringType(),True), \
    StructField("education_type",StringType(),True), \
    StructField("assignment_status", StringType(), True), \
    StructField("math_score", IntegerType(), True), \
    StructField("reading_score", IntegerType(), True), \
    StructField("writing_score", IntegerType(), True) \
    ])

    cwd = os.getcwd()
    dirname = os.path.dirname(cwd)
    input_file = "file://"+ dirname + "/inputfile/StudentsPerformance.csv"
    output_path = "file://"+ dirname + "/output"
    result_1_path = output_path + "/result_1"
    result_2_path = output_path + "/result_2"
    result_3_path = output_path + "/result_3"

    try:
        task_1 = read_data(spark,input_file)
    except Exception as e:
        print("Getting error in the read_data function",e)
    try:
        task_2 = result_1(task_1)
    except Exception as e:
        print("Getting error in the task_2 function",e)
    try:
        task_3 = result_2(task_1)
    except Exception as e:
        print("Getting error in the task_3 function",e)
    try:
        task_4 = result_3(task_1)      
    except Exception as e:  
        print("Getting error in the task_4 function",e)

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
