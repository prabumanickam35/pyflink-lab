### BELOW IS A STARTER TEMPLATE AND WORKING CODE THAT READS DATA FROM INOUT AND OUTPUTS only the fields: cid, type and heartrates (already in json format) ###

### MODIFY THE CODE BELOW TO COMPLETE THE TASK A and B ###

import os

from pyflink.table import (StreamTableEnvironment, DataTypes)
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table.expressions import col
from pyflink.table.udf import udtf
from pyflink.common import Row
from datetime import datetime
env = StreamExecutionEnvironment.get_execution_environment()

table_env = StreamTableEnvironment.create(stream_execution_environment=env)


def get_input_table_ddl(input_table_name):
    return  """CREATE TABLE {0} (
                 cid STRING,
                `type` STRING,
                 ts BIGINT,
                 heartrates ARRAY<ROW<heartrate INT, ts BIGINT, `type` STRING>>,
                 model STRING
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/heart_rate_flink/data/input/events.json',
            'format' = 'json'
            )""".format(input_table_name)


### Output table for taskA ###
def get_output_table_ddl_task_A(output_table_name_taskA):
    return   """CREATE TABLE {0} (
                cid STRING ,
                `type` STRING ,       
                heartrate  INT ,          
                hr_time    TIMESTAMP(3)   
            ) WITH (
            'connector' = 'filesystem',
            'path' = 'file:///opt/heart_rate_flink/data/output/taskA_result',
            'format' = 'json'
            )""".format(output_table_name_taskA)



def main():

    # tables names
    input_table_name = "input_heart_rate_data"
    # output_table_name = "transformed_heart_rate_data"
    output_table_name_taskA = "transformed_heart_rate_data_A"

    ### drop tables if they exist

    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(input_table_name))
    table_env.execute_sql(""" DROP TABLE IF EXISTS {0} """.format(output_table_name_taskA))

    # create the tables for input and output by executing the DDL
    table_env.execute_sql(get_input_table_ddl(input_table_name))
    table_env.execute_sql(get_output_table_ddl_task_A(output_table_name_taskA))

    # Read from table to begin transformations
    tab1 = table_env.from_path(input_table_name)

    @udtf(result_types=[DataTypes.STRING(), DataTypes.STRING(), DataTypes.INT(), DataTypes.TIMESTAMP(3)])
    def flatten(row : Row) -> Row:
        for hr in row.heartrates or ():
            yield row.cid, hr.type, hr.heartrate, datetime.fromtimestamp(hr.ts/1000)

    # Insert table  
    flat_result = tab1.filter(col("cid") == "5f2cc245-9c8d-4c40-b764-9210d0e2ffb1") \
            .filter(col("heartrates").is_not_null) \
            .flat_map(flatten) #join_lateral
    
    flat_result.execute().print()
    final_result = flat_result.execute_insert(output_table_name_taskA)
    
    final_result.wait()

if __name__ == "__main__":
    main()
