"""Task 3 Python"""

import pyodbc

import math

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
my_doc_db = mongo_client[cosmos_database_name]
# The name of the collection you are querying
my_orders = my_doc_db[cosmos_collection_name]
my_drivers = my_doc_db["Drivers"]
my_docs = my_orders.find({}, {'_id':0})
# Prints all documents in the collection



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

def get_order_items_sql(connection, order_id):
    """Shows all the records for a given table."""
    cursor = connection.cursor()
    cursor.execute("SELECT * \n"
                   + "FROM [pizza].[order_items] as p\n"
                   + "where [p].[order_id] = '" + str(order_id) + "' ")
    rows = cursor.fetchall()
    itemsList = []
    for row in rows:
       itemsList.append({'Pizza_Name' : row[2]  , 'Quantity': row[3] , 'Price_Each' :  float(row[4])})
    return(itemsList)

def get_customer_details(connection, customer_id):
    """Shows all the records for a given table."""
    cursor = connection.cursor()
    cursor.execute("SELECT * \n"
                   + "FROM [pizza].[customers] as c\n"
                   + "where [c].[customer_id] = '" + str(customer_id) + "' ")
    rows = cursor.fetchall()
    cust_dets = {}
    for row in rows:
       cust_dets.update({'first_Name' : row[1]  , 'last_Name': row[2] , 'phone' :  row[3], 'address':row[4], 'post_code':row[5]})
    return(cust_dets)

def create_Delivery_Doc(connection, order_id, customer_id):
    """Shows all the records for a given table."""
    itemsList = {}
    customer_list = get_customer_details(connection,customer_id)
    order_list = get_order_items_sql(connection, order_id)
    itemsList.update({'customer_name' : customer_list['first_Name'] + " " + customer_list['last_Name'], 
                      'customer_address' : customer_list['address'],
                      'customer_postcode' : customer_list['post_code'],
                      'Pizzas' : order_list,
                      'Total_Cost' : total_Cost(order_list),
                      'Driver' : allocate_Driver(customer_list['post_code'])
                      })
    return(itemsList)

def allocate_Driver(post_code):
    order_delivery_zone = roundDown(post_code)
    driver = my_drivers.find_one({"delivery_Code": order_delivery_zone}, {'_id':0})
    return driver

def total_Cost(order_list):
    total = 0
    for order in order_list:
        total += order['Quantity'] * order['Price_Each']
    return total

def create_Cooking_Doc(connection, order_id):
    """Shows all the records for a given table."""
    cursor = connection.cursor()
    cursor.execute("SELECT * \n"
                   + "FROM [pizza].[order_items] as p\n"
                   + "where [p].[order_id] = '" + str(order_id) + "' ")
    rows = cursor.fetchall()
    itemsList = []
    for row in rows:
       itemsList.append({'Pizza_Name' : row[2]  , 'Quantity': row[3] , 'Price_Each' :  float(row[4])})
    return(itemsList)
    


def show_records_for_day(connection, day):
    """Shows all the records for a given table."""
    cursor = connection.cursor()
    cursor.execute("SELECT * \n"
                   + "FROM [pizza].[orders] as p \n"
                   + "LEFT OUTER JOIN [dbo].[order_date_to_days] as v On p.order_date = v.order_date \n"
                   + "where [v].[order_day] = '" + str(day) + "' ")
    rows = cursor.fetchall()
    for row in rows:
        my_orders.insert_one(
            {
                "order_id" : row[0],
                "customer_Info" : get_customer_details(connection,row[1]),
                "order_date" : str(row[2]),
                "items" : get_order_items_sql(connection,row[0]),
                "delivery_Docket" : create_Delivery_Doc(connection,row[0],row[1])
                
            }   
        )
        print(get_customer_details(connection,row[1])['first_Name'])

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

def roundDown(n):
    n = n[:-2] + '00'
    return int(n)

connection = connect_to_azure_sql_database()
show_records_for_day(connection, 2)

connection.close()

