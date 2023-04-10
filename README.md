# About client_data

This application is made by PySpark, with the IDE of Pycharm.

The environment is Spark version 3.3.2 and Java HotSpot(TM) 64-Bit Server VM, 1.8.0_361.

The goal of this project was to eliminate the retrieved sensitive data, which can occurs GDPR conflict. Also the initial columns names were not acceptable, regarding their short names, and less meaningful former names.

Initially every needed library was imported.

The first step was to build up this application with the getOrCreate command, named 'Sparkapp1'.

The next step was reading the file pathways, with the consideration of the appname.

Displayed the schema to see if there are any changes.

The next step was rename the id columns to avoid to overlapping names which occurs error code inside of spark.
# |Old name|New name|
#|--|--|
#|id|client_identifier|
#|btc_a|bitcoin_address|
#|cc_t|credit_card_type|

After I joined them together on the id columns already avoided the confusion of the column names.

Then I filtered them only to UK and NL as a countries as the task required it.

I dropped the columns from the dataframes, namely the sensitive personal data and of course the credit card data as well.

I wrote the 2 dataframes back to the file system as a cleaned dataframes with the overwrite method of Spark.

I specifyed the pathway of the files for supporting the functions later.

I made 3 functions as was desired between the requirements.

I displayed the schema, because of schema goals.

I built up logging function to track the activities and display the reactions from the logging function.
Used the Log4j object and I initialized the spark configuration as a base of the logging function.
