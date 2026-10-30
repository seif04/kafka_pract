from dlq_producer import send_to_dlq

class FakeMessage:
    def topic(self):
        return "orders"

    def partition(self):
        return 1

    def offset(self):
        return 10

event = {
    "event_id":"E_TEST",
    "order_id": 999,
    "amount": "banana"
}

error = ValueError("Invalid amount: banana")

fakemessage = FakeMessage()

send_to_dlq(event, error, fakemessage)