import json
import sys

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer

TOPIC_NAME = "employee_salary"

class SalaryConsumer:
    _wait = 0
    _count = 0
    _deserializer = StringDeserializer('utf_8')

    def __init__(self, host: str = "localhost", port: str = "29092",
                 group_id: str = 'foo'):
        self.conf = {'bootstrap.servers': f'{host}:{port}',
                     'group.id': group_id,
                     'enable.auto.commit': True,
                     'auto.offset.reset': 'earliest'}
        self.consumer = Consumer(self.conf)
        self.keep_runnning = True

    def consume(self, topics, processing_func):
        try:
            self.consumer.subscribe(topics)
            while self.keep_runnning:
                if self._wait >= 5:
                    self.keep_runnning = False
                    continue

                msg = self.consumer.poll(1.0)
                if msg is None:
                    print("Waiting...")
                    self._wait += 1

                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                else:
                    processing_func(msg, self._deserializer)
                    self._count += 1
                    print(f'{self._count} messages have received.')

        except KeyboardInterrupt:
            pass

        finally:
            self.consumer.close()

def persist_employee(msg, deserializer):
    emp = json.loads(deserializer(msg.value()))
    try:
        conn = psycopg2.connect(
            host="localhost",
            database="postgres",
            user="postgres",
            password="postgres")
        conn.autocommit = True

        # create a cursor
        cur = conn.cursor()
        cur.execute(f"""
                    insert into department_employee(
                        department
                        , department_division
                        , position_title
                        , hire_date
                        , salary
                        ) values (
                        '{emp['dpt']}'
                        , '{emp['div']}'
                        , '{emp['pos']}'
                        , '{emp['hr_date']}'
                        , {emp['salary']}
                        ) on conflict do nothing
                    """)
        cur.execute(f"""
                    insert into department_employee_salary(
                        department
                        , total_salary
                        ) values (
                        '{emp['dpt']}'
                        , {emp['salary']}
                        ) on conflict (department) do update set
                        total_salary = department_employee_salary.total_salary + {emp['salary']}
                    """)
        cur.close()

    except Exception as e:
        print(emp)
        print('error: ', e)


if __name__ == '__main__':
    consumer = SalaryConsumer(group_id="employee_salary")
    consumer.consume([TOPIC_NAME], persist_employee)