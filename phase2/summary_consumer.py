import statistics
from statistics import mean

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
        self.av_Dep = 0
        self.dep_dev = 0
        self.with_dev = 0
        self.av_with = 0
        self.dep_list = []
        self.with_list = []

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
                self.dep_list.append(message['amt'])
                self.av_Dep = round(statistics.mean(self.dep_list))
                if len(self.dep_list) > 1:
                    self.dep_dev = statistics.pstdev(self.dep_list)
            else:
                self.custBalances[message['custid']] -= message['amt']
                self.with_list.append(message['amt'])
                self.av_with = round(statistics.mean(self.with_list))
                if len(self.with_list) > 1:
                    self.with_dev = statistics.pstdev(self.with_list)
            print(self.custBalances)
            print(self.av_Dep)
            print(self.av_with)
            print(self.with_dev)
            print(self.dep_dev)


if __name__ == "__main__":
    c = XactionConsumer()
    c.handleMessages()
