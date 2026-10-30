from confluent_kafka import Producer
import json

producer = Producer({
    "bootstrap.servers" : "localhost:9092"
})

event = {
    "event_id": "E105",
    "order_id": 503,
    "amount": 80.0
}

event_bytes = json.dumps(event).encode("utf-8")

def devlivery_report(err,msg):
    if err is not None:
        print("delivery failed",err)
    else:
        print(msg.topic())
        print(msg.offset())
        print(msg.partition())

producer.produce(
    topic= "orders",
    value= event_bytes,
    callback= devlivery_report,
    key= b"503"

)           

producer.flush()