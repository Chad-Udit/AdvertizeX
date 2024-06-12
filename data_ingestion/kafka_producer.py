from kafka import KafkaProducer
import json

def produce_ad_impressions():
    producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
    ad_impressions = [
        {"ad_creative_id": "1", "user_id": "101", "timestamp": "2024-06-01T12:00:00Z", "website": "example.com"},
        # add more records or use sample data with pandas
    ]
    for ad in ad_impressions:
        producer.send('ad_impressions', ad)
    producer.flush()

if __name__ == "__main__":
    produce_ad_impressions()
