## Instructions to run: 

 Build the docker file using the following commands ```docker build . -t pyflink-lab:latest```
 
Run the following command to perform Task-A-filter-unwind ```docker run  -v {path/to/your/data/folder/}:/opt/heart_rate_flink/data pyflink-lab:latest  /etc/poetry/bin/poetry run python /opt/heart_rate_flink/src/heart_rate_taskA.py``` 

Run the following command to generate Task-B-group-aggregate ```docker run  -v {path/to/your/data/folder/}:/opt/heart_rate_flink/data  pyflink-lab:latest  /etc/poetry/bin/poetry run python /opt/heart_rate_flink/src/heart_rate_taskB.py``` 
