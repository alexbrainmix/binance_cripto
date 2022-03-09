from clickhouse_driver import Client
import time
import datetime
import json

class DBHelper():
 
    def __init__(self, host_name = "192.168.0.109"):
        
        self.client = Client(host=host_name)
        print(datetime.datetime.now())
        while self.client.connection.connected is False:
            time.sleep(1)
            self.client.connection.connect()
            print("Reconnect!")
        
        print(datetime.datetime.now())
        self.database_name = "default"

    def _convert_type_py_to_type_sql(self, key, type_py):
        if key.find("_time") != -1 :
            type_sql = "DateTime64(3, 'UTC')"
        elif isinstance(type_py, list):
            type_sql = "Array(Array(String))"
        elif isinstance(type_py, bool):
            type_sql = "UInt8"
        elif isinstance(type_py, int):
            type_sql = "UInt64"
        elif isinstance(type_py, float):
            type_sql = "Float32"
        elif isinstance(type_py, str):
            type_sql = "String"
        return type_sql

    def create_database(self, database_name):
        create = self.client.execute("CREATE DATABASE IF NOT EXISTS " + database_name)
        # print(create)
        if not create:
            print("Created database " + database_name)
            self.database_name = database_name

    def create_table(self, data_dict):
        table_name = data_dict["stream_type"].replace("@", "_")
        # table_name = data_dict["event_type"] + data_dict["symbol"]
        sql = "CREATE TABLE IF NOT EXISTS " + self.database_name + "." + table_name + " ( "
        order_by = False
        for key in data_dict:
            if order_by is False and key.find("_id") != -1 :
                order_by = key
            if key not in ["stream_type", "event_type", "symbol"]:
                field_srt = key + " " + self._convert_type_py_to_type_sql(key, data_dict[key]) + ", "
                sql += field_srt
                # print(field_srt)
                # print(type(data_dict[key]))
        sql += " timestamp DateTime64(3, 'UTC') DEFAULT toDateTime(now(), 'UTC') ) ENGINE = MergeTree() ORDER BY " + str(order_by)
        print(sql)
        self.client.execute(sql)

    def print_table(self, data_dict):
        pass

    def insert(self, data_dict):
        table_name = data_dict["stream_type"].replace("@", "_")
        sql = "INSERT INTO " + self.database_name + "." + table_name + " VALUES ("
        for key in data_dict:
            if key not in ["stream_type", "event_type", "symbol"]:
                field_srt = str(data_dict[key]) + ", "
                sql += field_srt
                # print(field_srt)
                # print(type(data_dict[key]))
        sql = sql[:-2] + ", " + str(int(round(time.time() * 1000))) + ")"
        # print(sql)
        self.client.execute(sql)

    def insertJSON(self, data_dict):
        sql = "INSERT INTO " + self.database_name + "." + data_dict["stream_type"].replace("@", "_") + " FORMAT JSONEachRow "
        del data_dict["stream_type"]
        sql += str(json.dumps(data_dict))
        sql = sql[:-1] + ', "timestamp": ' + str(int(round(time.time() * 1000))) + "}"
        # print(sql)
        res = self.client.execute(sql)
        return res

if __name__ == "__main__":
    database_name = "binance_test"
    db = DBHelper()
    db.create_database(database_name)
    # print(db.client.execute("SHOW DATABASES"))
    list_tables = []
    ethbtc_trade = {"stream_type": "ethbtc@trade", "event_time": 1584270072097, "trade_id": 168540814, "price": 0.023283, "quantity": 0.011, "buyer_order_id": 650467333, "seller_order_id": 650467271, "trade_time": 1584270072095, "is_market_maker": 0, "ignore": 1}
    list_tables.append(ethbtc_trade)
    ethusdt_trade = {"stream_type": "ethusdt@trade", "event_time": 1584270072097, "trade_id": 168540814, "price": 0.023283, "quantity": 0.011, "buyer_order_id": 650467333, "seller_order_id": 650467271, "trade_time": 1584270072095, "is_market_maker": 0, "ignore": 1}
    list_tables.append(ethusdt_trade)
    btcusdt_trade = {"stream_type": "btcusdt@trade", "event_time": 1584270072097, "trade_id": 168540814, "price": 0.023283, "quantity": 0.011, "buyer_order_id": 650467333, "seller_order_id": 650467271, "trade_time": 1584270072095, "is_market_maker": 0, "ignore": 1}
    list_tables.append(btcusdt_trade)

    for table in list_tables:
        db.create_table(table)
    # db.insert(dict_table)
    # db.insertJSON(dict_table)
    # print(db.client.execute("select * from system.one"))
