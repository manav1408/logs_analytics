from kafka import KafkaProducer
import argparse

parser = argparse.ArgumentParser(description='kafka producer')

# Add arguments
parser.add_argument('input_file_path', type=str, help='input file path')
parser.add_argument('server', type=str, help='server')
parser.add_argument('topic', type=str, help='topic to be used')


# Parse the command-line arguments
args = parser.parse_args()
# Access the parsed values
file_path = args.input_file_path
server = args.server
topic = args.topic

# server = 'localhost:9092'
# topic = 'de_logs_6'
# file_name = '40MBFile.log'
# file_path = f'/Users/manavmiddha/Documents/Studies/USF/Summer/Data_Engineering/HW2/logs_data/{file_name}'

producer = KafkaProducer(bootstrap_servers=server)



with open(file_path, 'r') as file:
    for line in file:
        producer.send(topic, line.encode('utf-8'))
producer.close()