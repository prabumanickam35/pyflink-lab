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