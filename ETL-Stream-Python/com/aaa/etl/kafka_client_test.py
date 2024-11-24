'''
Created on 2023. 1. 22.

@author: joseph
'''
'''
from ctypes import CDLL
CDLL("C:\Anaconda3\Lib\site-packages\confluent_kafka.libs\librdkafka-09f4f3ec.dll")
'''
from confluent_kafka import Consumer

if __name__ == '__main__':
    conf = {'bootstrap.servers': 'localhost:9092',
            'group.id': 'test-group',
            'auto.offset.reset': 'earliest'}
    consumer = Consumer(conf)

    running = True

    '''
    topic_unempl_ann
    topic_house_income_ann
    topic_tax_exemp_ann
    topic_civil_force_ann
    topic_pov_ann
    topic_gdp_ann
    topic_unempl_mon
    topic_earn_Construction_mon
    topic_earn_Education_and_Health_Services_mon
    topic_earn_Financial_Activities_mon
    topic_earn_Goods_Producing_mon
    topic_earn_Leisure_and_Hospitality_mon
    topic_earn_Manufacturing_mon
    topic_earn_Private_Service_Providing_mon
    topic_earn_Professional_and_Business_Services_mon
    topic_earn_Trade_Transportation_and_Utilities_mon
    위 토픽 값 중에 하나를 택하여 토픽을 설정합니다.
    '''

    consumer.subscribe(['topic_unempl_mon'])

    while(running):
        msg = consumer.poll(timeout=1.0)
        if msg is None: continue

        if msg.error():
            print(msg.error())
        else:
            print(msg.value().decode('utf-8'))

    consumer.close()