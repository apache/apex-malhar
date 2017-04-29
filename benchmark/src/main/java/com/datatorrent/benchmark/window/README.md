Windowed Operator Benchmark Readme

Introduction
The Windowed operator benchmark are applications to test the performance of windowed operator. 
This benchmark applications only available for Malhar 3.7.0 or later versions. 
The source code is located inside the malhar/benchmark. There are two applications, 
one is benchmark for WindowedOperator, another is for KeyedWindowedOperator. 

Steps of compile
- Get source code of malhar
- Compile the malhar with all module option "-Pall-modules": "mvn clean install -Pall-modules -DskipTests"

Run the benchmark and check the result in cluster
- Start apex cli
- Launch application "WindowedOperatorBenchmarkApp" or "KeyedWindowedOperatorBenchmarkApp". Get application id, for example "application_1487803614053_10401"
- Use command "connect" to connect to the application
- Use command "list-operators" to list operators of the application
- Find the operator with name "output". Get the value of "host" field. for example "node1:8041"
- Go to the resource manager and click "Nodes" link in the left panel
- Find the node by "Node Address" which run the operator "output", click the link of "Node HTTP Address" column. for example "node1:8042"
- Expand the "Tools" on the left panel and click "Local logs"
- Click the "container/" link
- Select the application folder. The folder name is same as the application id
- Click the container and then click the log file 
- Open the log. you will find the benchmark which log in the log file for example: 
  "total: count: 7356000; time: 278453; average: 26417; period: count: 400000; time: 10005; average: 39980"
  Each log of benchmark includes “total” section and “period” section. 
  Total section counts all time from application started to log time. 
  "period" counts the time from previous log time to current log time. 
- The period average can help to determine if the rate is stable or not. The total average suppose to trends to the period average if application is stable
  The valid rate should be the stable rate
- Suggest to run this application several times to eliminate any temporary interference by environment.
