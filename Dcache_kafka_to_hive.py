import json

import findspark
findspark.init()

from pyspark.sql import SparkSession


import logging
logger = logging.getLogger()

logger.warn('*'*100 + ' this is a warning to check that the logging is working')

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json
from pyspark.sql.functions import schema_of_json
from pyspark.sql.functions import json_tuple
from pyspark.sql.functions import col
from pyspark.sql.functions import lit
from pyspark.sql.types import StringType
import time

#all the necessary functions for the module to function are imported

"""all the possible entries that the jsonstrings
coming from Kafka can have are defined 
and the field that will do the partitioning 
of the tables is also defined"""
INPUT_FIELDS = [
    'date', 'msgType', 'cellName', 'session', 'subject', 'initiator',
    'transferPath', 'queuingTime', 'cellDomain', 'isP2p', 'transferTime',
    'storageInfo', 'transferSize', 'localEndpoint', 'protocolInfo',
    'cellType', 'fileSize', 'pnfsid', 'billingPath', 'isWrite', 'status',
    'owner', 'clientChain', 'mappedGID', 'sessionDuration', 'mappedUID',
    'client', 'locations', 'transaction'
]

PARTITION_FIELD = 'partition_date'

#this function starts an spark session with the necessary packages for the application to work propperly
def get_spark_session(loglevel='INFO'):
    spark = SparkSession.builder.appName('kafka')\
    .enableHiveSupport().getOrCreate()
    sc = spark.sparkContext
    
    log4jLogger = sc._jvm.org.apache.log4j
    log = log4jLogger.LogManager.getLogger(__name__)
    
    log4jLogger.LogManager.getLogger("org.apache.spark").setLevel(getattr(log4jLogger.Level, loglevel))
    log4jLogger.LogManager.getLogger("org.apache.spark.streaming").setLevel(getattr(log4jLogger.Level, loglevel))
    log4jLogger.LogManager.getLogger("org.apache.kafka").setLevel(getattr(log4jLogger.Level, loglevel))

    return spark
    

def parse_json_value(dataframe):
    """
    parses the json string that is inside the dataframe, the new dataframe has all the possible
    columns that the json string can have, depending on the msgType the fields that will be filled will be different,
    all the columns that aren't on the structure of that peciffic msgType will be left as None in the row, finally
    it forces the columns to have the correct datatype

    Returns: 
        Parsed dataframe
    """
    allTypes = INPUT_FIELDS

    df1 = dataframe \
    .selectExpr("Cast(value AS STRING)")
    
    df2 = df1.select(json_tuple(df1.value, *allTypes).alias(*allTypes))

    df3 = (
        df2
        .select(*allTypes, json_tuple(df2.status, "msg", "code")
            .alias("status_msg", "status_code")
        )
        .select(*(allTypes + ['status_msg', 'status_code']),
            json_tuple(df2.protocolInfo, "protocol", "port", "host")
            .alias("protocolInfo_protocol", "protocolInfo_port", "protocolInfo_host")
        )
    )

    return (
        df3
        .withColumn("isP2p", col("isP2p").cast("boolean"))
        .withColumn("transferTime", col("transferTime").cast("float"))
        .withColumn("transferSize", col("transferSize").cast("float"))
        .withColumn("protocolInfo_port", col("protocolInfo_port").cast("int"))
        .withColumn("fileSize", col("fileSize").cast("float"))
        .withColumn("status_code", col("status_code").cast("int"))
        .withColumn("mappedGID", col("mappedGID").cast("int"))
        .withColumn("sessionDuration", col("sessionDuration").cast("int"))
        .withColumn("mappedUID", col("mappedUID").cast("int"))
        .withColumn("queuingTime", col("queuingTime").cast("int"))
    )



"""
insert...: it selects all the rows of the df parsed on the previous function that have that speciffic msgType and selects
only the usefull columns for it, then it creates a globalTempView (it has to be global because of how the foreachbatch() of 
pyspark.datastreamwriter works) and then it inserts the data on the corresponding table while creating the partition value

Arguments: 
    dataset: the dataframe that must be parsed with the function parse_json_value 
    table: name of the table in which the data is going to be inserted
    sparksession: the session that is able to connect with the tables

returns: 
    none
"""
    

def insertTransfers(dataset, table, spark):
    transfers = ['date', 'msgType', 'cellName', 'session', 'subject', 'initiator', 'transferPath', 'queuingTime', 'cellDomain', 'isP2p', 'transferTime', 'storageInfo', 'transferSize', 'localEndpoint', 'protocolInfo_protocol', 'protocolInfo_port', 'protocolInfo_host', 'cellType', 'fileSize', 'pnfsid', 'billingPath', 'isWrite', 'status_msg', 'status_code']
    dataset.select(transfers).createOrReplaceGlobalTempView("TempTransfers")
    spark.sql(f"""INSERT INTO {table} PARTITION ({PARTITION_FIELD})
    SELECT *, SUBSTR(date, 1, 10) as partition_date
    FROM global_temp.TempTransfers WHERE msgType = 'transfer'""")

def insertRequests(dataset, table, spark):
    requests = ['date', 'owner', 'msgType', 'clientChain', 'mappedGID', 'cellName', 'session', 'subject', 'transferPath', 'sessionDuration', 'storageInfo', 'cellType', 'fileSize', 'mappedUID', 'queuingTime', 'cellDomain', 'client', 'pnfsid', 'billingPath', 'status_msg', 'status_code']
    dataset.select(requests).createOrReplaceGlobalTempView("TempRequests")
    spark.sql(f"""INSERT INTO {table} PARTITION ({PARTITION_FIELD}) 
    SELECT *, SUBSTR(date, 1, 10) as partition_date
    FROM global_temp.TempRequests WHERE msgType = 'request'""")

def insertCinta(dataset, table, spark):
    cinta = ['date', 'msgType', 'transferTime', 'cellName', 'session', 'storageInfo', 'cellType', 'fileSize', 'queuingTime', 'cellDomain', 'locations', 'pnfsid', 'transaction', 'billingPath', 'status_msg', 'status_code']
    dataset.select(cinta).createOrReplaceGlobalTempView("TempCinta")
    spark.sql(f"""INSERT INTO {table} PARTITION ({PARTITION_FIELD})
    SELECT *, SUBSTR(date, 1, 10) as partition_date FROM global_temp.TempCinta
    WHERE msgType = 'store' or msgType = 'restore'""")

def insertRemoves(dataset, table, spark):
    removes = ['date', 'owner', 'msgType', 'clientChain', 'mappedGID', 'cellName', 'session', 'subject', 'transferPath', 'sessionDuration', 'cellType', 'fileSize', 'mappedUID', 'queuingTime', 'cellDomain', 'client', 'pnfsid', 'billingPath', 'transaction', 'status_msg', 'status_code']
    dataset.select(removes).createOrReplaceGlobalTempView("TempRemoves")
    spark.sql(f"""INSERT INTO {table} PARTITION ({PARTITION_FIELD})
    SELECT *, SUBSTR(date, 1, 10) as partition_date
    FROM global_temp.TempRemoves WHERE msgType = 'remove'""")





class Tables:
    """
    This class is able to create and erase tables on a hive/hdfs database.

    Arguments: 
        database: database that will be used on the hive server
        sparksession: the session needed to connect into the hive server.
    """
    def __init__(self, database, spark=None):
        if spark is None:
            spark = get_spark_session()
        self.spark = spark
        self.spark.sql(f"use {database}")
        
    def create_transfer(self, name):
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {name}(
                    date STRING,
                    msgType STRING, 
                    cellName STRING,
                    session STRING,
                    subject STRING,
                    initiator STRING,
                    transferPath STRING,
                    queuingTime INTEGER,
                    cellDomain STRING,
                    isP2p BOOLEAN,
                    transferTime REAL,
                    storageInfo STRING,
                    transferSize REAL,
                    localEndpoint STRING,
                    protocolInfo_protocol STRING,
                    protocolInfo_port INTEGER,
                    protocolInfo_host STRING,
                    cellType STRING,
                    fileSize REAL,
                    pnfsid STRING,
                    billingPath STRING,
                    isWrite STRING,
                    status_msg STRING,
                    status_code INTEGER
                    )
                    PARTITIONED BY (partition_date STRING) 
                    STORED AS PARQUET""")
    
    def create_request(self, name):        
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {name}(
                    date STRING,
                    owner STRING,
                    msgType STRING,
                    clientChain STRING,
                    mappedGID INTEGER,
                    cellName STRING,
                    session STRING,
                    subject STRING,
                    transferPath STRING,
                    sessionDuration REAL,
                    storageInfo STRING,
                    cellType STRING,
                    fileSize REAL,
                    mappedUID INTEGER,
                    queuingTime REAL,
                    cellDomain STRING,
                    client STRING,
                    pnfsid STRING,
                    billingPath STRING,
                    status_msg STRING,
                    status_code INTEGER)
                    PARTITIONED BY (partition_date STRING) 
                    STORED AS PARQUET""")
    
    def create_storage(self, name):
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {name}(
                    date STRING,
                    msgType STRING,
                    transferTime REAL,
                    cellName STRING,
                    session STRING,
                    storageInfo STRING,
                    cellType STRING,
                    fileSize REAL,
                    queuingTime REAL,
                    cellDomain STRING,
                    locations STRING,
                    pnfsid STRING,
                    transaction STRING,
                    billingPath STRING,
                    status_msg STRING,
                    status_code INTEGER) 
                    PARTITIONED BY (partition_date STRING) 
                    STORED AS PARQUET""")
        
    def create_remove(self, name):
        self.spark.sql(f"""CREATE TABLE IF NOT EXISTS {name}(
                    date STRING,
                    owner STRING,
                    msgType STRING,
                    clientChain STRING,
                    mappedGID INTEGER,
                    cellName STRING,
                    session STRING,
                    subject STRING,
                    transferPath STRING,
                    sessionDuration REAL,
                    cellType STRING,
                    fileSize REAL,
                    mappedUID INTEGER,
                    queuingTime REAL,
                    cellDomain STRING,
                    client STRING,
                    pnfsid STRING,
                    billingPath STRING,
                    transaction STRING,
                    status_msg STRING,
                    status_code INTEGER) 
                    PARTITIONED BY (partition_date STRING) 
                    STORED AS PARQUET""")
        
    def delete_tables(self, List):
        for i in List:
            self.spark.sql(f"DROP TABLE {i}")
            
    def show(self):
        self.spark.sql("SHOW TABLES").show()


class Streaming:
    """
    This class defines all the streaming process to the tables in the hive/hdfs server and starts the pyspark.sql.streaming.datastreamreader.

    Arguments:
        database: database that will be used on the hive server
        ...Table: the table in which the data of the ... msgTypes will be stored
        kafkaBootstrap: the code of the kafkaBootstrap server where the data of the streaming is read
        suscribePattern: the kafka pattern where the data that of the streaming is read
    """
    def __init__(self, database, transferTable, requestTable, storageTable, removeTable, kafkaBootstrap, suscribePattern, spark):
        self.database = database
        self.transferTable = str(transferTable)
        self.requestTable = str(requestTable)
        self.storageTable = str(storageTable)
        self.removeTable = str(removeTable)
        self.all_tables = [
            self.transferTable, self.requestTable,
            self.storageTable, self.removeTable
        ]
        self.spark = spark
        self.df = (
            self.spark
            .readStream.format("kafka")
            .option("kafka.bootstrap.servers", kafkaBootstrap)
            .option("subscribePattern", suscribePattern)
            .option("includeHeaders", "true")
            .option("failOnDataLoss" , "false")
            .load()
        )

    def to_hive(self, secondsBetweenBatches, checkpointLocation):
        """
        Streams the data recieved at the pyspark.sql.streaming.datastreamreader to the hive tables, optimally only does one write, it can happen that it streams twice in the time that it is given.

        Arguments:
            secondsBetweenBatches: the time between ending one batch and starting the next one
            checkpointLocation: folder in which the checkpoint files of the streaming will be stored.
                checkpointLocation with / at the start for absolute HDFS paths, file://path/to/file for non-HDFS locations

        Returns:
            none
        """
        self.spark.sql(f"use {self.database}")
        
        def forEachBatch(dataframe, batchid):
            """
            Function that must be defined to be used as an option in the pyspark.sql.streaming.datastreamwriter, defines the processing that
            the streamwriter must do to the data

            Arguments:
                dataframe: the dataframe that pyspark.sql.streaming.datastreamwriter will give to the function.
                batchid: the id of the corresponging batch.
            (all the arguments are given directly by the self.df.writeStream.foreachBatch function)
            """
    
            newDataframe = parse_json_value(dataframe)
        
            insertTransfers(newDataframe, self.transferTable, self.spark)
        
            insertRequests(newDataframe, self.requestTable, self.spark)
        
            insertCinta(newDataframe, self.storageTable, self.spark)
        
            insertRemoves(newDataframe, self.removeTable, self.spark)
            
        self.res = (
            self.df
            .writeStream.foreachBatch(forEachBatch)
            .option("checkpointLocation" , checkpointLocation)
            .trigger(processingTime=f'{secondsBetweenBatches} seconds')
        )

        self.query = self.res.start()
        self.query.awaitTermination(2*secondsBetweenBatches)
        self.query.stop()
            
            
    def stop_streaming(self):
        self.query.stop()
        

    def repartition(self, partitions=None):
        """
        Merge the files backing up the tables into one file 
        per partition, i.e. one file per day

        Arguments:
            partitions: list, list of partitions, e.g. ['2025-03-26', '2025-03-27', ...]
            
        Returns:
            None
        """
        self.spark.sql(f"use {self.database}")
        for table_name in self.all_tables:
            if partitions is None:
                # if partitions is not informed, get all of them
                partitions = (
                    self.spark.sql(f"show partitions {self.database}.{table_name}")
                    .rdd.map(lambda row: row[0].split("=")[1]).collect()
                )

            for partition in partitions:
                # Select all the elements in the partition
                df = self.spark.sql(f"""select * from {self.database}.{table_name} 
                              where {PARTITION_FIELD} = '{partition}'""")

                # Repartition them and create a table to put them in
                repartitioned_df = df.repartition(1)

                repartitioned_df.drop(PARTITION_FIELD).write.mode("overwrite").saveAsTable("temporal")
                    
                self.spark.sql(f"""INSERT OVERWRITE TABLE {self.database}.{table_name} PARTITION({PARTITION_FIELD} = '{partition}')
                            SELECT * FROM temporal""")