### BELOW IS A STARTER TEMPLATE AND WORKING CODE THAT READS DATA FROM INOUT AND OUTPUTS only the fields: cid, type and heartrates (already in json format) ###

### MODIFY THE CODE BELOW TO COMPLETE THE TASK A and B ###

import os

from pyflink.table import (StreamTableEnvironment, DataTypes)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import udtf
from pyflink.table.window import Tumble
from pyflink.table.window import Over
from pyflink.table.expressions import lit
from pyflink.common import Row
from datetime import datetime
env = StreamExecutionEnvironment.get_execution_environment()

table_env = StreamTableEnvironment.create(stream_execution_environment=env)


### Input table that can be used for Task B ###
def get_input_table_ddl_task_B(output_table_name_taskB):
    return   """CREATE TABLE {0} (
                cid STRING,
                `type` STRING,
                heartrate INT,
                hr_time TIMESTAMP(3),
                WATERMARK FOR hr_time as hr_time - INTERVAL '10' MINUTES
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/heart_rate_flink/data/output/taskA_result',
            'format' = 'json'
            )""".format(output_table_name_taskB)


### Output table for taskB ###
def get_output_table_ddl_task_B(output_table_name_taskA):
    return   """CREATE TABLE {0} (
                cid STRING ,
                hr_time    TIMESTAMP(3),
                average_heart_rate  INT
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/heart_rate_flink/data/output/taskB_result',
            'format' = 'json'
            )""".format(output_table_name_taskA)



def main():

    # tables names
    input_table_name = "transformed_heart_rate_data_A"
    output_table_name_taskB = "transformed_heart_rate_data_B"

    ### drop tables if they exist

    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(input_table_name))
    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(output_table_name_taskB))

    # create the tables for input and output by executing the DDL
    table_env.execute_sql(get_input_table_ddl_task_B(input_table_name))
    table_env.execute_sql(get_output_table_ddl_task_B(output_table_name_taskB))

    # Read from table to begin transformations
    tab1 = table_env.from_path(input_table_name)

    # Insert table  
    window_series = tab1.window(Tumble.over(lit(10).minutes).on(col("hr_time")).alias("time_window")) \
            .group_by(col("cid"), col("time_window")) \
            .select(col("cid"), col("time_window").start.alias("time_frame_start"), col("heartrate").avg.alias("average_heart_rate"))
    window_series.execute().print()
    final_result = window_series.execute_insert(output_table_name_taskB)
    
    final_result.wait()

if __name__ == "__main__":
    main()
