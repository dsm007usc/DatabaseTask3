"""Task 3 Python"""

import pyodbc

def connect_to_azure_sql_database():
  """Connects to an Azure SQL database."""
  server = 'task3headoffice.database.windows.net'
  database = 'Head_Office'
  username = 'dsm007'
  password = 'Password123'   
  driver= '{ODBC Driver 18 for SQL Server}'
  
  connection_string = 'Driver='+driver+';Server=tcp:'+server+',1433;Database='+database+';Uid='+username+';PWD='+password+';Encrypt=yes;TrustServerCertificate=no;Connection Timeout=30;'
  connection = pyodbc.connect(connection_string)
  return connection

def show_records(connection, day):
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
show_records(connection, 1)

connection.close()

