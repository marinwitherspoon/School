# PART 2-  Create Star/Snowflake Schema

# Authors : Marin Witherspoon and Sumit Hawal
# Course  : CS5200 DBMS SuF
# Date    : 02/08/2023

# loading required packages
library('RSQLite')
library(XML)
library(RMySQL, quietly=T)

# connect to database hosted on db4free server
dbcon <-  dbConnect(RMySQL::MySQL(), 
                    user = 'sql9637042', 
                    password = 'DjcYLGYggR',
                    dbname = 'sql9637042', 
                    host = 'sql9.freemysqlhosting.net', 
                    port = 3306)

# drop the tables if exists
dbExecute(dbcon, "DROP TABLE IF EXISTS rep_facts;")
dbExecute(dbcon, "DROP TABLE IF EXISTS rep_customer;")
dbExecute(dbcon, "DROP TABLE IF EXISTS rep_sailestxn;")
dbExecute(dbcon, "DROP TABLE IF EXISTS rep_product;")

dbExecute(dbcon, "DROP TABLE IF EXISTS prod_facts;")
dbExecute(dbcon, "DROP TABLE IF EXISTS prod_reps;")
dbExecute(dbcon, "DROP TABLE IF EXISTS prod_customers;")

# Create tables for prod fact table
dbExecute(dbcon,
          "CREATE TABLE prod_reps (
          rid INT PRIMARY KEY ,
          first_name TEXT,
          last_name TEXT,
          region TEXT
          )")

dbExecute(dbcon,
          "CREATE TABLE prod_customers (
          cid INT PRIMARY KEY ,
          customer TEXT,
          country TEXT
          )")

dbExecute(dbcon,
          "CREATE TABLE prod_facts (
          pid INT PRIMARY KEY ,
          pname TEXT,
          total_quant INT,
          quart_quant INT,
          date DATE,
          region_quant INT,
          rid INT,
          cid INT,
          FOREIGN KEY (rid) REFERENCES prod_reps (rid),
          FOREIGN KEY (cid) REFERENCES prod_customers (cid)
          )")

# disconnect the database
dbDisconnect(dbcon)
