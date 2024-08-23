import json
import sys

import psycopg2
from confluent_kafka import Consumer, KafkaError, KafkaException
from confluent_kafka.serialization import StringDeserializer

TOPIC_NAME = "employee_cdc"

class CdcConsumer:
    _count = 0
    _last_id = 0
    _wait = False
    _deserializer = StringDeserializer('utf_8')

    def __init__(self, kafka_host="localhost", kafka_port="29092",
                 db_host="localhost", db_port=5434, db_database="source",
                 db_user="source", db_password="source", group_id: str = 'foo'):
        conf = {'bootstrap.servers': f'{kafka_host}:{kafka_port}',
                     'group.id': group_id,
                     'auto.offset.reset': 'earliest'}
        self._consumer = Consumer(conf)
        self._conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_database,
                user=db_user,
                password=db_password)
        self._conn.autocommit = True

    def consume(self, topics):
        try:
            self._consumer.subscribe(topics)

            while True:
                msg = self._consumer.poll(1.0)

                if msg is None:
                    if not self._wait:
                        print("Waiting...")
                        self._wait = True

                elif msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                         (msg.topic(), msg.partition(), msg.offset()))
                    elif msg.error():
                        raise KafkaException(msg.error())
                    
                    self._wait = False

                else:
                    emp = json.loads(self._deserializer(msg.value()))
                    self.update_employee(emp)
                    self._count += 1
                    self._last_id = emp['action_id']
                    print(f'{self._count} messages have received.')
                    self._consumer.commit()
                    self._wait = False

        except KeyboardInterrupt:
            pass

        finally:
            self._consumer.close()

    def update_employee(self, emp):
        try:
            cur = self._conn.cursor()
            cur.execute(f"""
                        create table if not exists employees(
                            emp_id SERIAL primary key,
                            first_name VARCHAR(100),
                            last_name VARCHAR(100),
                            dob DATE,
                            city VARCHAR(100)
                        );
                        """)
            
            if emp['action'] == 'insert':
                cur.execute(f"""
                            insert into employees(emp_id, first_name, last_name, dob, city)
                            values (
                                {emp['emp_id']}
                                , '{emp['first_name']}'
                                , '{emp['last_name']}'
                                , '{emp['dob']}'
                                , '{emp['city']}'
                                ) on conflict (emp_id) do nothing
                            """)
                
            elif emp['action'] == 'update':
                cur.execute(f"""
                            update employees
                            set
                                first_name = '{emp['first_name']}',
                                last_name = '{emp['last_name']}',
                                dob = '{emp['dob']}',
                                city = '{emp['city']}'
                            where emp_id = {emp['emp_id']}
                            """)
                
            else:
                cur.execute(f"""
                            delete from employees
                            where emp_id = {emp['emp_id']}
                            """)
                
            cur.close()

        except Exception as e:
            print(emp)
            print('error: ', e)

if __name__ == '__main__':
    consumer = CdcConsumer(group_id="employee_cdc")
    consumer.consume([TOPIC_NAME])