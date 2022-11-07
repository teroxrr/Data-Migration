# Import required modules
import os
from pathlib import Path
from pyspark.sql import SparkSession


# Declare folders
input_folder = 'input/'
processed_folder = 'processed/'
skipped_folder = 'skipped/'


def spark_session_builder(name):
    """Create Spark Session"""

    return (
        SparkSession.builder.enableHiveSupport()
        .appName(name)
        .getOrCreate()
    )


def check_dtypes(df, file_no_extension):
    """Check data types for given dataframe
    Input:
        df : Spark Dataframe
        file_no_extension : String. Name of file, without extension

    Output:
        correct_types : Boolean. True if all column types match expected types, False otherwise.
    """

    # Columns of each table
    departments_cols = {'id': 'integer', 
                        'department':'string'}

    jobs_cols = {'id': 'integer', 
                 'jobs':'string'}

    hired_employees_cols = {'id': 'integer', 
                            'name':'string', 
                            'datetime':'string', 
                            'department_id': 'integer', 
                            'job_id': 'integer'}


    # Select columns depending on processed file
    if file_no_extension == 'departments':
        columns = departments_cols
    elif file_no_extension == 'jobs':
        columns = jobs_cols
    elif file_no_extension == 'hired_employees':
        columns = hired_employees_cols
    
    
    # Check each column's data type
    correct_types = True
    for type in df.dtypes:
        if columns[type[0]] != type[1]:
            correct_types = False

    return correct_types         

    
def app():
    
    # Create Spark Session
    spark = spark_session_builder("migration")

    # Iterate over input files
    for filename in os.scandir(input_folder):
        if filename.is_file():
            
            # Get file name without extension
            file_no_extension = Path(filename).stem
            
            # Read CSV
            df = spark.read.format("csv").load(filename)

            # Check data types
            correct_types = check_dtypes(df, file_no_extension)

            if correct_types:
                # Write into table 
                df.write.mode("append").format("AVRO").insertInto(f"datalake.{file_no_extension}")
                # Move file to processed folder
                os.rename(filename, filename.replace('input', 'processed'))
            
            elif not correct_types:
                # Move file to skipped folder
                os.rename(filename, filename.replace('input', 'skipped'))


if __name__ == "__main__":
    app()