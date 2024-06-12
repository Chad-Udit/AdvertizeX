from kafka import KafkaProducer
import avro.schema
import avro.io
import io

def produce_bid_requests():
    schema = avro.schema.parse(open("bid_request_schema.avsc", "r").read())
    producer = KafkaProducer(bootstrap_servers='localhost:9092')
    bid_requests = [
        {"user_id": "101", "auction_details": "details", "ad_targeting_criteria": "criteria"},
        # add more records or use sample data with pandas
    ]
    for bid in bid_requests:
        bytes_writer = io.BytesIO()
        encoder = avro.io.BinaryEncoder(bytes_writer)
        writer = avro.io.DatumWriter(schema)
        writer.write(bid, encoder)
        raw_bytes = bytes_writer.getvalue()
        producer.send('bid_requests', raw_bytes)
    producer.flush()

if __name__ == "__main__":
    produce_bid_requests()
