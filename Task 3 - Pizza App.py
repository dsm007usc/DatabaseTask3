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
my_doc_db = mongo_client[cosmos_database_name]
# The name of the collection you are querying
my_orders = my_doc_db[cosmos_collection_name]
my_drivers = my_doc_db["Drivers"]
my_docs = my_orders.find({}, {'_id':0})
# Prints all documents in the collection

store_Id = 1106100

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

def connect_to_joes_pizza_sql():
  """Connects to an Azure SQL database."""
  server = 'task3headoffice.database.windows.net'
  database = 'Joes_Pizza_Place'
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
    dilivery_doc = {}
    customer_list = get_customer_details(connection,customer_id)
    order_list = get_order_items_sql(connection, order_id)
    order_Total = total_Cost(order_list)
    driver = allocate_Driver(customer_list['post_code'])
    dilivery_doc.update({'customer_name' : customer_list['first_Name'] + " " + customer_list['last_Name'], 
                      'customer_address' : customer_list['address'],
                      'customer_postcode' : customer_list['post_code'],
                      'Pizzas' : order_list,
                      'Total_Cost' : order_Total,
                      'Driver' : driver,
                      'Comission' : round((driver['commission_Rate']/100) * order_Total, 2)
                      })
    return(dilivery_doc)

def allocate_Driver(post_code):
    order_delivery_zone = roundDown(post_code)
    driver = my_drivers.find_one({"delivery_Code": order_delivery_zone}, {'_id':0})
    return driver

def roundDown(n):
    n = n[:-2] + '00'
    return int(n)

def total_Cost(order_list):
    total = 0
    for order in order_list:
        total += order['Quantity'] * order['Price_Each']
    return round(total,2)

def create_Cooking_Doc(connection, order_id, customer_id):
    """Shows all the records for a given table."""
    cooking_doc = {}
    customer_list = get_customer_details(connection,customer_id)
    order_list = get_order_items_sql(connection, order_id)
    cooking_doc.update({'customer_name' : customer_list['first_Name'] + " " + customer_list['last_Name'], 
                      'Pizzas' : order_list,
                      'Total_Cost' : total_Cost(order_list),
                      })
    return(cooking_doc) 

def show_records_for_day(connection, my_sql, date):
    """Shows all the records for a given table."""
    cursor = connection.cursor()
    cursor.execute("SELECT * \n"
                   + "FROM [pizza].[orders] as O \n"
                   + "where [O].[order_date] = '" + str(date) + "' ")
    rows = cursor.fetchall()
    for row in rows:
        my_orders.insert_one(
            {
                "order_id" : row[0],
                "customer_Info" : get_customer_details(connection,row[1]),
                "order_date" : str(row[2]),
                "items" : get_order_items_sql(connection,row[0]),
                "delivery_Docket" : create_Delivery_Doc(connection,row[0],row[1]),
                "cooking_Docket" : create_Cooking_Doc(connection,row[0],row[1])
            }   
        )
    todays_Date = str(row[2])
    daily_Summary(todays_Date, my_sql)
    upload_summary_to_headoffice(connection,my_sql,todays_Date)
    print(str(date) + " is completed")

def daily_Summary(date,connection):
    """total driver commission for the day"""
    total_orders = 0
    daily_Items = []
    days_drivers = {}
    
    for doc in my_orders.find({"order_date" : date}):
        total_orders += 1
        daily_Items += doc["items"]
        
        if days_drivers.get(doc["delivery_Docket"]["Driver"]['driver_Name']) == None:
            days_drivers.update({doc["delivery_Docket"]["Driver"]['driver_Name']: doc["delivery_Docket"]["Comission"]})
        else:
            days_drivers[doc["delivery_Docket"]["Driver"]['driver_Name']] += doc["delivery_Docket"]["Comission"]
            
    total_sales = total_Cost(daily_Items)
    days_drivers_str = ""
    for drivers in days_drivers:
        days_drivers_str += str(drivers) + " : $" + str(days_drivers.get(drivers)) + " ,"
    
    fav_pizza = find_Fav_Pizza(daily_Items)
    fav_pizza_str = ""
    for pizza in fav_pizza:
        fav_pizza_str += pizza + " , "
        
    cursor = connection.cursor()
    try:
        cursor.execute("""INSERT INTO dbo.Daily_Summary (Summary_Date, Store_Id, Total_Orders,Total_Sales,Drivers_Com,Favourite_Pizza) 
                   VALUES (?, ?, ?, ?, ?, ?)""",(date, store_Id, total_orders,total_sales,days_drivers_str,fav_pizza_str))
        connection.commit()
    except Exception as e:
        """already exist"""
        
def upload_summary_to_headoffice(head_Office_con, joes_pizza_con, date):
    jp_cursor = joes_pizza_con.cursor()
    jp_cursor.execute("""SELECT * FROM dbo.Daily_Summary
                   WHERE Summary_Date = ?""",(date))
    daily_sum = jp_cursor.fetchone()
    
    ho_cursor = head_Office_con.cursor()
    ho_cursor.execute("""INSERT INTO pizza.summary (store_id, summary_date, total_sales, total_orders, best_product) 
                   VALUES (?, ?, ?, ?, ?)""",(store_Id, date,daily_sum[3],daily_sum[2],daily_sum[5]))
    head_Office_con.commit()

def find_Fav_Pizza(daily_Items):
    favourates_List = []
    pizza_List = {"Hawaiian" : 0, "Margherita" : 0, "Meatlovers" : 0 , "Pepperoni" : 0 , "Supreme" : 0 , "Vegetarian" : 0}
    for items in daily_Items:
        pizza_name = items['Pizza_Name']
        pizza_List[pizza_name]= pizza_List.get(pizza_name) + items['Quantity']
    maximum = max(pizza_List.values())
    for i in pizza_List:
        if pizza_List.get(i) == maximum:
            favourates_List.append(i)
    return favourates_List
    
def clear_Head_Office(connection):
    cursor = connection.cursor()
    cursor.execute("DELETE FROM pizza.summary WHERE store_id = 1106100")
    connection.commit()

def run_all_Days(head_office, my_sql):
    cursor = head_office.cursor()
    cursor.execute("""SELECT order_date
    FROM pizza.orders
    GROUP BY order_date
    ORDER BY order_date ASC""")
    rows = cursor.fetchall()
    for row in rows:
        show_records_for_day(head_office,my_sql,row[0])
    print("All Orders Complete")

connection = connect_to_azure_sql_database()
my_sql = connect_to_joes_pizza_sql()
my_orders.drop()
clear_Head_Office(connection)
#show_records_for_day(connection, my_sql, "2023-08-15")
run_all_Days(connection, my_sql)
connection.close()
my_sql.close()

