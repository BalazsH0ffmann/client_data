# import the needed libraries
import pyspark
from pyspark.sql import *



# building up the spark app with the alias
spark1 = SparkSession.builder.appName('Sparkapp1').getOrCreate()



# reading the pathways
df1 = spark1.read.option("header", True).csv('/Users/Balazs.Hoffmann/Downloads/dataset_one.csv')
df2 = spark1.read.option("header", True).csv('/Users/Balazs.Hoffmann/Downloads/dataset_two.csv')

# display schema
df1.printSchema()
df2.printSchema()

#Rename the first id column
df1 = df1.withColumnRenamed("id","id2")


# join them together
dff = df1.join(df2, df1. id2 == df2. id, "left")


#use UK and Netherlands as a country
dff1 = dff.filter(dff["country"] == "United Kingdom")
dff2 = dff.filter(dff["country"] == "Netherlands")



#Drop columns from the 2 dataframe
dff1 = dff1.drop("first_name")
dff1 = dff1.drop("last_name")
dff1 = dff1.drop("cc_n")
dff2 = dff2.drop("first_name")
dff2 = dff2.drop("last_name")
dff2 = dff2.drop("cc_n")


#Rename the columns to
# |Old name|New name|
#|--|--|
#|id|client_identifier|
#|btc_a|bitcoin_address|
#|cc_t|credit_card_type|

dff1 = dff1.withColumnRenamed("id2","client_identifier2")
dff1 = dff1.withColumnRenamed("btc_a","bitcoin_address")
dff1 = dff1.withColumnRenamed("cc_t","credit_card_type")
dff2 = dff2.withColumnRenamed("id","client_identifier")
dff2 = dff2.withColumnRenamed("btc_a","bitcoin_address")
dff2 = dff2.withColumnRenamed("cc_t","credit_card_type")

#Save the dataframes
dff1 = dff1.write.mode("overwrite").option("header",True).csv("/Users/Balazs.Hoffmann/Downloads/data_uk")
dff2 = dff2.write.mode("overwrite").option("header",True).csv("/Users/Balazs.Hoffmann/Downloads/data_nl")

#Specify the directories
dff1_dir = "/Users/Balazs.Hoffmann/Downloads/data_uk"
dff2_dir = "/Users/Balazs.Hoffmann/Downloads/data_nl"

#Display the schema one more time
dff1.printSchema()
dff2.printSchema()

#Function to get the data pathway at data_uk
def get_pathway_data_uk():
    return dff1_dir

#Function to get the data pathway at data_nl
def get_pathway_data_nl():
    return dff2_dir

#Function to filter the country
def filter_the_country():
    return dff.filter(dff["country"] == (input("")))

# building up the logging functions
class Log4j(object):
    """Wrapper class for Log4j JVM object.
    :param spark: SparkSession object.
    """

    def __init__(self, spark):
        # get spark app details with which to prefix all messages
        conf = spark.sparkContext.getConf()
        app_id = conf.get('spark.app.id')
        app_name = conf.get('spark.app.name')

        log4j = spark._jvm.org.apache.log4j
        message_prefix = '<' + app_name + ' ' + app_id + '>'
        self.logger = log4j.LogManager.getLogger(message_prefix)

    def error(self, message):
        """Log an error.
        :param: Error message to write to log
        :return: None
        """
        self.logger.error(message)
        return None

    def warn(self, message):
        """Log a warning.
        :param: Warning message to write to log
        :return: None
        """
        self.logger.warn(message)
        return None

    def info(self, message):
        """Log information.
        :param: Information message to write to log
        :return: None
        """
        self.logger.info(message)
        return None
