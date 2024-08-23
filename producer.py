import csv
import json
import sys

from confluent_kafka.serialization import StringSerializer
from confluent_kafka import Producer

TOPIC_NAME = "employee_salary"

class SalaryProducer:
    def __init__(self, host="localhost", port="29092"):
        self.conf = {'bootstrap.servers': f'{host}:{port}'}
        self.producer = Producer(self.conf)

    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    def produce(self, topic_name, value):
        self.producer.produce(topic=topic_name,
                              value=value,
                              on_delivery=self.delivery_callback)

class CsvReader:
    def read_csv(self, csv_file):
        with open(csv_file, newline='') as csvfile:
            rows = []
            reader = csv.reader(csvfile, delimiter=',')

            for row in reader:
                rows.append(row)

            return rows

if __name__ == '__main__':
    serializer = StringSerializer('utf_8')
    reader = CsvReader()
    producer = SalaryProducer()
    count = 0

    lines = reader.read_csv('employee_salaries_2.csv')
    for line in lines[1:]:
        emp = {}

        if line[0] in ['ECC', 'CIT', 'EMS'] and int(line[5][-4:]) >= 2010:
                emp = {'dpt': line[0],
                       'div': line[1],
                       'pos': line[3],
                       'hr_date': line[5],
                       'salary': int(float(line[7]))}
                
                producer.produce(TOPIC_NAME,
                                 value=serializer(json.dumps(emp)))
                count += 1

    producer.producer.poll(10000)
    producer.producer.flush()
    print(f'{count} messages have sent.')