import statistics
from statistics import mean

from kafka import KafkaConsumer, TopicPartition, conn
from json import loads
from sqlalchemy import create_engine
import pymysql


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

        self.mySql_engine = create_engine('mysql+pymysql://root:zipcoder@localhost/kafka')
        self.conn = self.mySql_engine.connect()
        # Go back to the readme.

    def handleMessages(self):
        for message in self.consumer:
            message = message.value
            print('{} received'.format(message))
            self.ledger[message['custid']] = message
            ch_message = list(message.values())
            new_values = tuple(ch_message)
            # self.conn.execute("DROP TABLE IF EXISTS transaction;")
            # self.conn.execute("CREATE TABLE transaction(custid Integer not null,type varchar(250) not null,date int,"
            #                   "amt int);")
            self.conn.execute("INSERT INTO transaction VALUES (%s,%s,%s,%s)", new_values)

            if message['custid'] not in self.custBalances:
                self.custBalances[message['custid']] = 0
            if message['type'] == 'dep':
                self.custBalances[message['custid']] += message['amt']
            else:
                self.custBalances[message['custid']] -= message['amt']
            print(self.custBalances)


def drop_tbl_transaction():
    mysql_engine = create_engine('mysql+pymysql://root:zipcoder@localhost/kafka')
    # cur = mysql_engine.cursor()
    # cur.execute("""
    con = mysql_engine.connect()
    con.execute("""
        DROP TABLE IF EXISTS transaction;
    #     CREATE TABLE transaction(
    #             custid Integer not null,
    #             type varchar(250) not null,
    #             date int,
    #             amt int);
    # """)
    #CREATE TABLE transaction(custid Integer not null,type varchar(250) not null,date int,amt int);
    # conn = psycopg2.connect("host=localhost dbname=airflow_test user=postgres")
    # cur = conn.cursor()
    # cur.execute("""
    # conn.commit()


def create_tbl_transaction():
    mysql_engine = create_engine('mysql+pymysql://root:zipcoder@localhost/kafka')
    # cur = mysql_engine.cursor()
    # cur.execute("""
    con = mysql_engine.connect()
    con.execute("""
        #DROP TABLE IF EXISTS transaction;
        CREATE TABLE transaction(
                custid Integer not null,
                type varchar(250) not null,
                date int,
                amt int);
    """)


if __name__ == "__main__":
    drop_tbl_transaction()
    create_tbl_transaction()
    c = XactionConsumer()
    c.handleMessages()
