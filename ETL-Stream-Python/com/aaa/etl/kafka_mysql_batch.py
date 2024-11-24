'''
Created on 2023. 1. 22.

@author: joseph
'''
import configparser

from pyspark.sql import SparkSession 
from pyspark.sql.functions import col
from pyspark.sql.functions import from_csv 


class Kafka2MySQLBatch(object):
    '''
    classdocs
    '''

    def __init__(self):
        '''
        Constructor
        '''
        self._config = configparser.ConfigParser()
        self._config.read('resources/SystemConfig.ini')

        appName = self._config['SPARK_CONFIG']['spark.batch.name']
        self._spark = SparkSession.builder.master("local[*]").appName(appName).getOrCreate()

    def getDF(self, kafka_topic):
        df = self._spark.read.format("kafka")\
            .option("kafka.bootstrap.servers", self._config['KAFKA_CONFIG']['kafka.brokerlist'])\
            .option("subscribe", kafka_topic)\
            .option("startingOffsets", self._config['KAFKA_CONFIG']['kafka.resetType'])\
            .load()
        df = df.selectExpr("CAST(value AS STRING) as column").filter(col("column").startswith('date') == False)

        return df

    def saveDF2MysqlDB(self, df, tableName):
        csv_schema = """date DATE,
                        value FLOAT,
                        state STRING,
                        id STRING,
                        title STRING,
                        frequency_short STRING,
                        units_short STRING,
                        seasonal_adjustment_short STRING"""
        
        dfs = df.select(from_csv(df.column, csv_schema).alias("EntityPojo"))\
            .selectExpr("EntityPojo.date", "EntityPojo.value", \
                        "EntityPojo.state", "EntityPojo.id", \
                        "EntityPojo.title", "EntityPojo.frequency_short", \
                        "EntityPojo.units_short", "EntityPojo.seasonal_adjustment_short")

        dfs.show()
        dfs.printSchema()

        mysql_user = self._config['MYSQL_CONFIG']['mysql.user']
        mysql_password = self._config['MYSQL_CONFIG']['mysql.password']

        jdbc_properties = {"user": mysql_user, "password": mysql_password}

        mysql_host_url = self._config['MYSQL_CONFIG']['mysql.host.url']
        dfs.write.mode("overwrite").jdbc(mysql_host_url, tableName, properties=jdbc_properties)


        ''' MongoDB에 저장할 경우
        mongo_output_url = self._config['MONGO_CONFIG']['mongodb.output.uri']
        mongo_database = self._config['MONGO_CONFIG']['mongodb.output.database']
        
        dfs.write.format("mongo").mode("overwrite").option("uri", mongo_output_url)\
            .option("database", mongo_database).option("collection", tableName).save()
        '''

        self._spark.stop()