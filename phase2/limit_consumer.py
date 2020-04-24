from kafka import KafkaConsumer, TopicPartition, conn
from json import loads
from sqlalchemy import create_engine


class XactionConsumer:
    def __init__(self):
        self.consumer = KafkaConsumer('bank-customer-events',
                                      bootstrap_servers=['localhost:9092'],
                                      # auto_offset_reset='earliest',
                                      value_deserializer=lambda m: loads(m.decode('ascii')))
        # These are two python dictionarys
        # Ledger is the one where all the transaction get posted
        self.ledger = {}
        # custBalances is the one where the current blance of each customer
        # account is kept.
        self.custBalances = {}
        self.limit = -5000

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            if message['type'] == 'wth' and self.custBalances < self.limit:
                print(message['custid'], message['amt'])
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)
"""

LimitConsumer

LimitConsumer should keep track of the customer ids that have current balances greater or equal to -5000 
to the constructor. The intro suggests -5000 for eaxmple, but you should be able set that with a parameter to the 
class' Constructor """

if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()