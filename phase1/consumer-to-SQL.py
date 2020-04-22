import sqlalchemy
from kafka import KafkaConsumer, TopicPartition, conn
from json import loads
import psycopg2
from sqlalchemy.dialects.postgresql import psycopg2
from sqlalchemy import Column, Integer, String
from sqlalchemy.ext.declarative import declarative_base

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

        # self.engine = sqlalchemy.create_engine('mysql+pymysql://root:zipcoder@localhost/kafka')
        # self.conn = psycopg2.conn("host=localhost dbname=kafka user=postgres")
        #self.engine = sqlalchemy.create_engine('postgres+psycopg2://root:zipcoder@localhost/kafka')
        self.conn = create_engine('postgres+psycopg2://root:zipcoder@localhost/kafka')
        # Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            # add message to the transaction table in your SQL usinf SQLalchemy
            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)
            print()
            with self.engine.connect() as con:
                with con.begin():
                    # cur = conn.cursor()
                    con.execute("INSERT INTO transaction(custid, type, amt) VALUES (int(message['custid']),"
                                " str(message['type']), int(message['amt'])")
# >>> conn.execute(addresses.insert(), [
# ...    {'user_id': 1, 'email_address' : 'jack@yahoo.com'},
# ...    {'user_id': 1, 'email_address' : 'jack@msn.com'},
# ...    {'user_id': 2, 'email_address' : 'www@www.org'},
# ...    {'user_id': 2, 'email_address' : 'wendy@aol.com'},
# ... ])

# if __name__ == "__main__":
#     c = XactionConsumer()
#     c.handleMessages()

# def create_tbl_consumer_tran():
#     conn = psycopg2.connect("host=localhost dbname=kafka user=postgres")
#     cur = conn.cursor()
#     cur.execute("""
#         drop table if exists transaction;
#         CREATE TABLE transaction(
#                 id = Column(Integer, primary_key=True)
#                 custid = Column(Integer)
#                 type = Column(String(250), nullable=False)
#                 date = Column(Integer)
#                 amt = Column(Integer)
#     )
#     """)
#     conn.commit()
#
#
# def insert_to_tbl():
#     with open("how to store the data from a dict format", 'r') as f:
#         next(f)
#         cur.copy_from(f, 'transaction', sep=',')
#         conn.commit()
def connect_pst():
    psycopg2.connect("host=localhost dbname=kafka user=postgres")
    cur = conn.cursor()
    with open("/Users/mtessema/Desktop/PY/TSLA.csv", 'r') as f:
        next(f)
        cur.copy_from(f, 'stock', sep=',')
        conn.commit()
