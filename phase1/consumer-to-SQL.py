import sqlalchemy
from kafka import KafkaConsumer, TopicPartition, conn
from json import loads
import psycopg2
from sqlalchemy.dialects.postgresql import psycopg2
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, session

Base = declarative_base()


class transaction(Base):
    __tablename__ = 'transaction'
    id = Column(Integer, primary_key=True)
    custid = Column(Integer)
    type = Column(String(250))
    date = Column(Integer)
    amt = Column(Integer)


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
        # THE PROBLEM is every time we re-run the Consumer, ALL our customer
        # data gets lost!
        # add a way to connect to your database here.

        self.engine = create_engine('mysql+pymysql://root:zipcoder@localhost/kafka')
        session = sessionmaker(bind=self.engine)
        session = session()

        # Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            message_sql= transaction(custid=message['custid'], type=message['type'], date=message['date'])
            session.ass(message_sql)
            session.commit()
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)
            print()



