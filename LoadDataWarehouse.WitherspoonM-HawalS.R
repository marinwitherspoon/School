# PART 2-  Create Star/Snowflake Schema

# Authors : Marin Witherspoon and Sumit Hawal
# Course  : CS5200 DBMS SuF
# Date    : 08/08/2023

# loading required packages
library('RSQLite')
library(XML)
library(RMySQL, quietly=T)
library(lubridate)

#Table create
#########################################################

# connect to database hosted on db4free server
dbcon <-  dbConnect(RMySQL::MySQL(), 
                    user = 'sql9637042', 
                    password = 'DjcYLGYggR',
                    dbname = 'sql9637042', 
                    host = 'sql9.freemysqlhosting.net', 
                    port = 3306)

#creating product fact tables----------
# drop the tables if exists
dbExecute(dbcon, "DROP TABLE IF EXISTS prod_facts;")
dbExecute(dbcon, "DROP TABLE IF EXISTS prod_reps;")
dbExecute(dbcon, "DROP TABLE IF EXISTS prod_customers;")

dbExecute(dbcon,
          "CREATE TABLE prod_reps (
          rid INT PRIMARY KEY ,
          fname TEXT,
          lname TEXT,
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
          quarter TEXT,
          quart_quant INT,
          region TEXT,
          reg_quant INT,
          ridFK INT,
          cidFK INT,
          FOREIGN KEY (ridFK) REFERENCES prod_reps (rid),
          FOREIGN KEY (cidFK) REFERENCES prod_customers (cid)
          )")

#creating rep fact tables----------
# drop the tables if exists
dbExecute(dbcon, "SET foreign_key_checks = 0;")
dbExecute(dbcon, "DROP TABLE IF EXISTS rep_customer;")
dbExecute(dbcon, "DROP TABLE IF EXISTS rep_product;")
dbExecute(dbcon, "DROP TABLE IF EXISTS rep_facts;")
dbExecute(dbcon, "SET foreign_key_checks = 1;")

dbExecute(dbcon,
          "CREATE TABLE rep_customer (
          cid INT PRIMARY KEY ,
          customer TEXT,
          country TEXT)")

dbExecute(dbcon,
          "CREATE TABLE rep_product (
          pid INT PRIMARY KEY,
          quantity INT,
          amount INT)")

dbExecute(dbcon,
          "CREATE TABLE rep_facts (
          rid INT PRIMARY KEY,
          fname TEXT,
          lname TEXT,
          region TEXT,
          quarter TEXT,
          quart_quant INT,
          date DATE,
          pname TEXT,
          quant INT,
          cidFK INT,
          pidFK INT,
          FOREIGN KEY (cidFK) REFERENCES rep_customer (cid),
          FOREIGN KEY (pidFK) REFERENCES rep_product (pid))")

#------------
#Table filling
###################################################

# reconnect with the loadXML database 
con <- dbConnect(RSQLite::SQLite(),'database.sqlite')

#table filling function
t_fill <- function(table,que) {
  #table: the table to be filled
  #que: The query from con to fill the table with
  
  #execute query from con
  f_qu <- dbGetQuery(con, que)
  
  #fill table
  dbWriteTable(dbcon,table,f_qu, append = TRUE, row.names = FALSE)}

#filling product fact tables-----------------------------

#fill prod_reps table
t_fill("prod_reps","SELECT * FROM reps")

#table fill prod_customers
t_fill("prod_customers","SELECT * FROM customers")

#fill prod_facts table
prod_facts_quer <- "SELECT pid, pname, 
                    SUM(quantity) OVER(PARTITION BY pname) AS Total_quant,
                    strftime('%Y-Q', date) || ((strftime('%m', date) - 1) / 3 + 1) AS quarter, 
                    SUM(quantity) OVER(PARTITION BY strftime('%Y-Q', date) || ((strftime('%m', date) - 1) / 3 + 1)) AS quart_quant,
                    region,
                    SUM(quantity) OVER(PARTITION BY region) AS reg_quant,
                    ridFK, cidFK
                    FROM salestxn 
                    JOIN products ON salestxn.pidFK = products.pid 
                    JOIN reps ON reps.rid = salestxn.ridFK"

t_fill("prod_facts",prod_facts_quer)

#filling rep fact tables----------

#filling rep_customers table
t_fill("rep_customer","SELECT * FROM customers")

#Filling rep_product table
rep_product_query <- "SELECT pid, quantity, amount FROM salestxn
                      JOIN products ON salestxn.pidFK = products.pid"
t_fill("rep_product",rep_product_query)

#filling rep_facts
rep_facts_query <- "SELECT rid,fname,lname, region,
                    strftime('%Y-Q', date) || ((strftime('%m', date) - 1) / 3 + 1) AS quarter, 
                    SUM(quantity) OVER(PARTITION BY strftime('%Y-Q', date) || ((strftime('%m', date) - 1) / 3 + 1)) AS quart_quant,
                    date, pname,
                    SUM(quantity) OVER(PARTITION BY pname) AS quant,
                    cidFK,pidFK
                    FROM reps
                    JOIN salestxn ON salestxn.pidFK = products.pid
                    JOIN products ON salestxn.pidFK = products.pid"

t_fill("rep_facts",rep_facts_query)
#-------------
# disconnect the database
dbDisconnect(dbcon)

# disconnect the database
dbDisconnect(dbcon)
