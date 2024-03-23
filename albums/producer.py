from kafka import KafkaProducer
import pandas as pd
import json

producer = KafkaProducer(
    bootstrap_servers='large-longhorn-11829-us1-kafka.upstash.io:9092',
    sasl_mechanism='SCRAM-SHA-256',
    security_protocol='SASL_SSL',
    sasl_plain_username='bGFyZ2UtbG9uZ2hvcm4tMTE4MjkkzXaxwwsp62_RthQykRR_H7Oy25BCSZF-2Yg',
    sasl_plain_password='YzNiMWYxMTYtZjk3NC00ZWMyLWEyZmEtZTk2MDdkZGIyMzgx',
    api_version_auto_timeout_ms=100000,    
)
tracks = pd.read_csv('albums.csv')

for dt in tracks.to_dict(orient='records'):
    data = json.dumps(dt).encode('utf-8')

    try:
        result = producer.send('albums', data).get(timeout = 60)    
        print("Message produced:", result)
    except Exception as e:
        print(f"Error producing message: {e}")
producer.close()