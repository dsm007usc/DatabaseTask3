"""Task 3 Python"""

import pyodbc

from pymongo import MongoClient
import json
# Get these values from the Azure portal page for your cosmos db account. Change the details to fit your database and collection
cosmos_user_name = "dmark"
cosmos_password = "cXfhNEQmVi343hQXU1WHzdJSDrIhksxsb6fODmS7WG4Cu0RAc5kn7V3yTqkrVYN8C3HdAlAq6TrrACDb3m07fQ=="
cosmos_url = "dmark.mongo.cosmos.azure.com:10255/" # use 10255 not 443
cosmos_database_name = "PizzaDocData"
cosmos_collection_name = "Orders"
uri = f'mongodb://{cosmos_user_name}:{cosmos_password}@{cosmos_url}?ssl=true&retrywrites=false&replicaSet=globaldb&maxIdleTimeMS=120000&appName=@{cosmos_user_name}@'
mongo_client = MongoClient(uri)
# This is the name of the Mongo compatible database
my_db = mongo_client[cosmos_database_name]
# The name of the collection you are querying
my_col = my_db[cosmos_collection_name]
my_docs = my_col.find({}, {'_id':0})
# Prints all documents in the collection
for doc in my_docs:
    print(doc['name'])


def connect_to_azure_sql_database():
  """Connects to an Azure SQL database."""
  server = 'task3headoffice.database.windows.net'
  database = 'Head_Office'
  username = 'dsm007'
  password = 'Password123'   
  driver='{ODBC Driver 17 for SQL Server}'
  
  connection_string = 'Driver='+driver+';Server=tcp:'+server+',1433;Database='+database+';Uid='+username+';PWD='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
  connection = pyodbc.connect(connection_string)
  return connection

def show_records_for_day(connection, day):
    """Shows all the records for a given table."""
    cursor = connection.cursor()
    cursor.execute("SELECT * \n"
                   + "FROM [pizza].[orders] as p \n"
                   + "LEFT OUTER JOIN [dbo].[order_date_to_days] as v On p.order_date = v.order_date \n"
                   + "where [v].[order_day] = '" + str(day) + "' ")
    rows = cursor.fetchall()
    for row in rows:
        print(row)

"""def insert_record(connection, mytable, myrecord):
    Inserts a given record into a given table.
    cursor = connection.cursor()
    cursor.execute("INSERT INTO "+mytable+" VALUES "+ str(myrecord)+";")
    connection.commit()"""

def general_query(connection):
    """Does a general quesry on the database. Be careful with sql injection!"""
    sql = input("Enter your query: ")
    cursor = connection.cursor()
    cursor.execute(sql)
    rows = cursor.fetchall()
    for row in rows:
        print(row)


connection = connect_to_azure_sql_database()
show_records_for_day(connection, 5)

connection.close()

