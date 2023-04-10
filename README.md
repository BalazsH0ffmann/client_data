# About client_data

This application is made by PySpark, with the IDE of Pycharm.

The environment is Spark version 3.3.2

Initially every needed library was imported.

The first step was to build up this application with the getOrCreate command.

The next step was reading the pathways, considering the appname.

Displayed the schema to see if there are any changes.

The next step was rename the id columns to avoid to overlapping names which occurs error code inside of spark.

After I joined them together on the id columns already avoided the confision of the column names.

Then I filtered them only to UK and NL as a countries as the task desired it.

I dropped the columns from the dataframes, namely the sensitive personal data and of course the credit card data as well.

I wrote the 2 dataframes back to the file system as a cleaned dataframes.

I specifyed the pathway of the files for supporting the functions later.

I made 3 functions as was desired between the requirements.

I built up logging function to track the activities and display the reactions from the logging function.
Used the Log4j object and I initialized the spark configuration as a base of the logging function.
