import pymysql

import os
from dotenv import load_dotenv, find_dotenv

_ = load_dotenv(find_dotenv()) # read local .env file
mysql_host = os.getenv('mysql_host')
mysql_user = os.getenv('mysql_user')
mysql_password = os.getenv('mysql_password')
mysql_database = os.getenv('mysql_database')

connection = pymysql.connect(host=mysql_host,
	user=mysql_user,
	password=mysql_password,
	database=mysql_database)


# query_string = 'SELECT 1;'
# print(query_execute(query_string))

# connection = pymysql.connect(host="localhost",
# 	user="<mysql_user>",
# 	password="<mysql_password>",
# 	database="<database_name>")

# try:
# 	cursor = connection.cursor()
# 	cursor.execute("select database();")
# 	db = cursor.fetchone()
# 	print("You're connected to database: ", db)
# except pymysql.Error as e:
# 	print("Error while connecting to MySQL", e)
# finally:
# 	cursor.close()
# 	connection.close()
# 	print("MySQL connection is closed")

