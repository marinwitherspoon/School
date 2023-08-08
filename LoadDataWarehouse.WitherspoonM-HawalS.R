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
dbExecute(dbcon, "DROP TABLE IF EXISTS prod_month;")

dbExecute(dbcon,
          "CREATE TABLE prod_reps (
          rid INT PRIMARY KEY ,
          fname TEXT,
          lname TEXT
          )")

dbExecute(dbcon,
          "CREATE TABLE prod_customers (
          cid INT PRIMARY KEY ,
          customer TEXT,
          country TEXT
          )")

dbExecute(dbcon,
          "CREATE TABLE prod_facts (
          pid INT PRIMARY KEY,
          pname TEXT,
          total_sold INT,
          YEAR INT,
          quarter TEXT,
          region TEXT,
          total_sold_per_region INT)")

dbExecute(dbcon,
          "CREATE TABLE prod_month (
          pid INT PRIMARY KEY,
          pname TEXT,
          month TEXT,
          total_sold_per_month INT)")

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
          total_sold INT,
          year INT,
          quarter TEXT,
          pname TEXT,
          total_sold_per_product INT)")

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

#filling product prod tables-----------------------------

#fill prod_reps table
t_fill("prod_reps","SELECT rid,fname,lname FROM reps")

#table fill prod_customers
t_fill("prod_customers","SELECT * FROM customers")

#fill prod_facts table
prod_facts_quer <- "SELECT
    total_sold.pid,
    total_sold.pname,
    total_sold.total_sold,
    quarter_and_year.year,
    quarter_and_year.quarter,
    region.region,
    region.total_sold_per_region
FROM (
    SELECT
        p.pid,
        p.pname,
        SUM(s.quantity) AS total_sold
    FROM salestxn s 
    JOIN products p ON p.pid = s.pidFK
    GROUP BY p.pid, p.pname) total_sold
LEFT JOIN (
    SELECT
        p.pid,
        STRFTIME('%Y', st.date) AS year,
        (CAST(STRFTIME('%m', st.date) AS INTEGER) - 1) / 3 + 1 AS quarter,
        SUM(st.quantity) AS total_sold_per_quarter_and_year
    FROM products p
    JOIN salestxn st ON p.pid = st.pidFK
    GROUP BY p.pid, year, quarter) quarter_and_year 
    ON total_sold.pid = quarter_and_year.pid
LEFT JOIN (
    SELECT
        p.pid,
        r.region,
        SUM(st.quantity) AS total_sold_per_region
    FROM products p
    JOIN salestxn st ON p.pid = st.pidFK
    JOIN reps r ON st.ridFK = r.rid
    GROUP BY p.pid, r.region) region ON total_sold.pid = region.pid;"

t_fill("prod_facts",prod_facts_quer)

month_data_query <- "SELECT
p.pid,
p.pname,
STRFTIME('%Y-%m', st.date) AS month,
SUM(st.quantity) AS total_sold_per_month
FROM products p
JOIN salestxn st ON p.pid = st.pidFK
GROUP BY p.pname, month"
t_fill("prod_month",month_data_query)
month_fill <- dbGetQuery(con,month_data_query)

#filling rep fact tables---------------------------------------

#filling rep_customers table
t_fill("rep_customer","SELECT * FROM customers")

#Filling rep_product table
rep_product_query <- "SELECT pid, quantity, amount FROM salestxn
                      JOIN products ON salestxn.pidFK = products.pid"
t_fill("rep_product",rep_product_query)

#filling rep_facts
rep_facts_query <- "SELECT
    rep_total.rid,
    rep_total.fname,
    rep_total.lname,
    rep_total.total_sold,
    quarter_and_year.year,
    quarter_and_year.quarter,
    product_total.pname,
    product_total.total_sold_per_product
FROM (
    SELECT
        r.rid,
        r.fname,
        r.lname,
        SUM(st.quantity) AS total_sold
    FROM reps r
    JOIN salestxn st ON r.rid = st.ridFK
    GROUP BY r.rid, r.fname, r.lname) rep_total
LEFT JOIN (
    SELECT
        r.rid,
        STRFTIME('%Y', st.date) AS year,
        (CAST(STRFTIME('%m', st.date) AS INTEGER) - 1) / 3 + 1 AS quarter,
        SUM(st.quantity) AS total_sold_per_quarter_and_year
    FROM reps r
    JOIN salestxn st ON r.rid = st.ridFK
    GROUP BY r.rid, year, quarter) quarter_and_year 
    ON rep_total.rid = quarter_and_year.rid
LEFT JOIN (
    SELECT
        r.rid,
        p.pname,
        SUM(st.quantity) AS total_sold_per_product
    FROM reps r
    JOIN salestxn st ON r.rid = st.ridFK
    JOIN products p ON st.pidFK = p.pid
    GROUP BY r.rid, p.pname) product_total ON rep_total.rid = product_total.rid;"

t_fill("rep_facts",rep_facts_query)
#-------------
# disconnect the database
dbDisconnect(dbcon)

# disconnect the database
dbDisconnect(dbcon)
