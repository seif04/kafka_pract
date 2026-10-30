from confluent_kafka import Producer
import json


producer = Producer({
    "bootstrap.servers":"localhost:9092"
})


def delivery_report(err,msg):
    global delivery_stat
    delivery_stat = False
    if err is not None:
        print(err)
    else:
        delivery_stat = True
        print("topic:",msg.topic())
        print("partiton:",msg.partition())
        print("offset:",msg.offset())    


def send_to_dlq(event,error,original_msg):

    dlq_dict = {

        "original_event": event,
        "error": str(error),
        "original_topic": original_msg.topic(),
        "original_partition": original_msg.partition(),
        "original_offset": original_msg.offset()
    }

    dlq_bytes = json.dumps(dlq_dict).encode("utf-8") 
    
    producer.produce(
    topic="orders.DLQ",
    key= event["event_id"].encode("utf-8"),
    value= dlq_bytes,
    callback=delivery_report
   )  

    remaining = producer.flush(timeout=10)

    if delivery_stat is False:
        raise Exception("DLQ delivery failed")
        
    if remaining > 0:
        raise RuntimeError("DLQ message was not delivered before timeout")
          

