We have 2 python files: 1 for producer and 1 for consumer

1. Producer: To run the producer file, please follow the following syntax to run in command line

python3 producer.py /Users/manavmiddha/Documents/Studies/USF/Summer/Data_Engineering/HW2/logs_data/2GBFile.log localhost:9092 de_logs_7 

Name of code file: producer.py
Input file name: /Users/manavmiddha/Documents/Studies/USF/Summer/Data_Engineering/HW2/logs_data/2GBFile.log
Server address Kafka Bootstrap server:  localhost:9092 
Topic name where data is being pushed: de_logs_7

Please change the code file, input file name, server address and topic name as per your requirement

2. Consumer: To run the producer file, please follow the following syntax to run in command line

spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0 consumer.py de_logs_6 localhost:9092 Users/manavmiddha/Documents/Studies/USF/Summer/Data_Engineering/temp


Name of code file:  consumer.py 
Input file name: Users/manavmiddha/Documents/Studies/USF/Summer/Data_Engineering/temp
Server address Kafka Bootstrap server:  localhost:9092 
Topic name where data is being pushed: de_logs_6

Please change the code file, input file name, server address and topic name as per your requirement
