import json
import sys

import psycopg2
from confluent_kafka.serialization import StringSerializer
from confluent_kafka import Producer

TOPIC_NAME = "employee_cdc"

class CdcProducer:
    _count = 0
    _last_id = 0
    _serializer = StringSerializer('utf_8')

    def __init__(self, kafka_host="localhost", kafka_port="29092",
                 db_host="localhost", db_port=5433, db_database="source",
                 db_user="source", db_password="source"):
        conf = {'bootstrap.servers': f'{kafka_host}:{kafka_port}'}
        self._producer = Producer(conf)
        self._conn = psycopg2.connect(
                host=db_host,
                port=db_port,
                database=db_database,
                user=db_user,
                password=db_password)
        self._conn.autocommit = True

    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))
    
    def read_cdc(self, last_id):
        try:
            cur = self._conn.cursor()
            cur.execute(f"""
                        select *
                        from employees_cdc
                        where action_id > {last_id}
                        order by action_id
                        """)
            return cur.fetchall()

        except Exception as e:
            print('error: ', e)

    def produce(self, topic):
        try:
            while True:
                actions = self.read_cdc(self._last_id)

                if actions:
                    for action in actions:
                        message = {'action_id': action[0],
                                'emp_id': action[1],
                                'first_name': action[2],
                                'last_name': action[3],
                                'dob': action[4].strftime('%Y-%m-%d'),
                                'city': action[5],
                                'action': action[6]}
                        
                        self._producer.produce(topic=topic,
                                    key=str(action[1]),
                                    value=self._serializer(json.dumps(message)),
                                    on_delivery=self.delivery_callback)
                        self._count += 1
                        self._last_id = action[0]
                        self._producer.poll(1)
                        self._producer.flush()
                        print(f'{self._count} messages have sent.')

        except KeyboardInterrupt:
            pass
        
if __name__ == '__main__':
    producer = CdcProducer()
    producer.produce(TOPIC_NAME)
        