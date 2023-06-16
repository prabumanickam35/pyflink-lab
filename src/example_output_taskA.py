### Output table that can be used for Task A ###
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