from pyflink.table import TableEnvironment
from pyflink.table import EnvironmentSettings
import mysql.connector
from pyflink.table.expressions import col
from elasticsearch import Elasticsearch
import os

class PyFlinkConnector:
    def __init__(self, topic_name, bootstrap_server, db_mysql, index_es):
        self.config = {
            'flink': {
                'jar_path': f'file://{os.getcwd()}/jars/flink-sql-connector-kafka-1.17.1.jar; \
                        file://{os.getcwd()}/jars/mysql-connector-java-8.0.22.jar; \
                        file://{os.getcwd()}/jars/flink-connector-jdbc-3.1.1-1.17.jar; \
                        file://{os.getcwd()}/jars/flink-connector-mysql-cdc-2.3.0.jar; \
                        file://{os.getcwd()}/jars/flink-sql-connector-elasticsearch7-3.0.1-1.17.jar'
            },

            'kafka': {
                'topic': f'{topic_name}',
                'bootstrap_server': f'{bootstrap_server}'
            },

            'mysql': {
                'host': 'localhost',
                'port': '3306',
                'database': f'{db_mysql}',
                'user': 'root',
                'password': 'admin',
                'url': f'jdbc:mysql://localhost:3306/{db_mysql}',
                "create_table_transactions": f"{os.getcwd()}/sql/mysql/create_table_transactions.sql",
                "create_table_sales_per_category": f"{os.getcwd()}/sql/mysql/create_table_sales_per_category.sql",
                "create_table_sales_per_day": f"{os.getcwd()}/sql/mysql/create_table_sales_per_day.sql",
                "create_table_sales_per_month": f"{os.getcwd()}/sql/mysql/create_table_sales_per_month.sql",
                "create_triggers_sales": f"{os.getcwd()}/sql/mysql/create_triggers_sales.sql",
                "create_database_mysql": f"{os.getcwd()}/sql/mysql/create_database_mysql.sql"
            },

            'elasticsearch': {
                'hosts': 'http://localhost:9200',
                'index': f'{index_es}'
            }
        }

    def read_sql_file(self, file_path):
        with open(file_path, 'r') as file:
            return file.read()

    def ddl_mysql(self):
        try:
            mydb = mysql.connector.connect(
                host=self.config['mysql']['host'],
                user=self.config['mysql']['user'],
                password=self.config['mysql']['password'],
            )
            cnx = mydb.cursor(buffered=True)
            create_database_mysql = f"CREATE DATABASE IF NOT EXISTS `{self.config['mysql']['database']}`"
            create_table_transactions = self.read_sql_file(
                self.config['mysql']['create_table_transactions'])
            create_table_sales_per_category = self.read_sql_file(
                self.config['mysql']['create_table_sales_per_category'])
            create_table_sales_per_day = self.read_sql_file(
                self.config['mysql']['create_table_sales_per_day'])
            create_table_sales_per_month = self.read_sql_file(
                self.config['mysql']['create_table_sales_per_month'])
            create_triggers_sales = self.read_sql_file(
                self.config['mysql']['create_triggers_sales'])
            cnx.execute(create_database_mysql)
            cnx.execute(f"USE `{self.config['mysql']['database']}`")
            cnx.execute(create_table_transactions)
            cnx.execute(create_table_sales_per_category)
            cnx.execute(create_table_sales_per_day)
            cnx.execute(create_table_sales_per_month)
            cnx.execute(create_triggers_sales)
            mydb.commit()
            print("===============Finished create tables MySQL===========================")
            cnx.close()
            mydb.close()
        except Exception as e:
            print("Error: " + str(e))

    def ddl_es(self):
        try:
            es = Elasticsearch(
                hosts=f"{self.config['elasticsearch']['hosts']}")
            settings = {
                "settings": {
                    "number_of_shards": 1,
                    "number_of_replicas": 1
                },
                "mappings": {
                    "properties": {
                        "transactionId": {"type": "keyword"},
                        "productId": {"type": "text"},
                        "productName": {"type": "text"},
                        "productCategory": {"type": "text"},
                        "productPrice": {"type": "double"},
                        "productQuantity": {"type": "integer"},
                        "productBrand": {"type": "text"},
                        "currency": {"type": "text"},
                        "customerId": {"type": "text"},
                        "transactionDate": {"type": "text"},
                        "paymentMethod": {"type": "text"},
                        "totalAmount": {"type": "double"},
                    }
                }
            }
            for field_name, field_definition in settings["mappings"]["properties"].items():
                field_definition["fields"] = {"keyword": {"type": "keyword"}}
            index_name = self.config['elasticsearch']['index']
            es.indices.create(index=index_name, body=settings)
            print("===============Finished create index ElasticSearch====================")
        except:
            print("Exist index!!!")

    def create_flink_stream(self):
        try:

            t_env = TableEnvironment.create(
                EnvironmentSettings.in_streaming_mode())
            t_env.get_config().set(
                "pipeline.jars", self.config['flink']['jar_path'])

            source_ddl = f"""
                        CREATE TABLE IF NOT EXISTS KafkaTable (
                            `transactionId` STRING,
                            `productId` STRING,
                            `productName` STRING,
                            `productCategory` STRING,
                            `productPrice` DECIMAL,
                            `productQuantity` BIGINT,
                            `productBrand` STRING,
                            `currency` STRING,
                            `customerId` STRING,
                            `transactionDate` STRING,
                            `paymentMethod` STRING,
                            `totalAmount` DECIMAL
                        ) WITH (
                            'connector' = 'kafka',
                            'topic' = '{self.config['kafka']['topic']}',
                            'properties.bootstrap.servers' = '{self.config['kafka']['bootstrap_server']}',
                            'scan.startup.mode' = 'earliest-offset',
                            'format' = 'json'
                        )
                        """

            t_env.execute_sql(source_ddl)
            return t_env
        except Exception as e:
            print("Error: " + str(e))

    def stream_transactions_elasticsearch(self):
        try:
            t_env = self.create_flink_stream()
            es_transactions = f"""
                CREATE TABLE IF NOT EXISTS ESTable (
                    `transactionId` STRING,
                    `productId` STRING,
                    `productName` STRING,
                    `productCategory` STRING,
                    `productPrice` DECIMAL,
                    `productQuantity` BIGINT,
                    `productBrand` STRING,
                    `currency` STRING,
                    `customerId` STRING,
                    `transactionDate` STRING,
                    `paymentMethod` STRING,
                    `totalAmount` DECIMAL,
                    PRIMARY KEY(transactionId) NOT ENFORCED
                ) WITH (
                    'connector' = 'elasticsearch-7',
                    'hosts' = '{self.config['elasticsearch']['hosts']}',
                    'index' = '{self.config['elasticsearch']['index']}'
                )
            """

            t_env.execute_sql(es_transactions)
            print(
                "===============Putting transactions data to ElasticSearch===============")
            t_env.execute_sql("""
                INSERT INTO ESTable
                SELECT * FROM KafkaTable
            """).wait()
        except Exception as e:
            print("Error: " + str(e))

    def stream_transactions_mysql(self):
        try:
            t_env = self.create_flink_stream()
            mysql_transactions = f"""
                CREATE TABLE IF NOT EXISTS JDBCTable (
                    `transactionId` STRING,
                    `productId` STRING,
                    `productName` STRING,
                    `productCategory` STRING,
                    `productPrice` DECIMAL,
                    `productQuantity` BIGINT,
                    `productBrand` STRING,
                    `currency` STRING,
                    `customerId` STRING,
                    `transactionDate` STRING,
                    `paymentMethod` STRING,
                    `totalAmount` DECIMAL,
                    PRIMARY KEY(transactionId) NOT ENFORCED
                ) WITH (
                    'connector' = 'jdbc',
                    'url' = '{self.config['mysql']['url']}',
                    'table-name' = 'Transactions',
                    'driver' = 'com.mysql.cj.jdbc.Driver',
                    'username' = 'root',
                    'password' = 'admin'
                )
            """
            t_env.execute_sql(mysql_transactions)
            print("===============Putting transactions data to MySQL===============")
            t_env.execute_sql("""
                INSERT INTO JDBCTable
                SELECT * FROM KafkaTable
            """).wait()
        except Exception as e:
            print("Error: " + str(e))
