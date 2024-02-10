from faker import Faker
import random
from datetime import datetime
from kafka import KafkaProducer
import json
import time
import uuid

class Generator_Sales_Data:
    def __init__(self, topic_name, bootstrap_servers):
        self.topic = topic_name
        self.bootstrap_servers = bootstrap_servers

    def generate_sales_transaction(self):
        user = Faker().simple_profile()
        return {
            "transactionId": str(uuid.uuid1()),
            "productId": random.choice(['product1', 'product2', 'product3', 'product4', 'product5', 'product6']),
            "productName": random.choice(['laptop', 'mobile', 'tablet', 'watch', 'headphone', 'speaker']),
            "productCategory": random.choice(['electronic', 'fashion', 'grocery', 'home', 'beauty', 'sports']),
            "productPrice": round(random.uniform(10,1000), 2),
            "productQuantity": random.randint(1, 10),
            "productBrand": random.choice(['apple', 'samsung', 'oneplus', 'mi', 'boat', 'sony']),
            "currency": random.choice(['USD', 'VND']),
            "customerId": user['username'],
            "transactionDate": datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S'),
            "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer'])
        }

    def main(self):
        producer = KafkaProducer(bootstrap_servers=[f'{self.bootstrap_servers}'])
        while True:
            print('------------------Writing data into Kafka Topic------------------')
            transaction = self.generate_sales_transaction()
            transaction['totalAmount'] = transaction['productPrice'] * transaction['productQuantity']
            producer.send(topic=self.topic, key=transaction['transactionId'].encode('utf-8'), value=json.dumps(transaction).encode('utf-8'))
            print('----------------------------Finished----------------------------')
            time.sleep(5)

if __name__ == "__main__":
    generator_data = Generator_Sales_Data()
    while True:
        generator_data.main()
        time.sleep(5)