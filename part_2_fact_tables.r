
library(RSQLite)
library(XML)


dbcon <- 'mysql.sqlite'
dbcon <- dbConnect(RSQLite::SQLite(), dbcon)


con <- dbConnect(RSQLite::SQLite(),'database.sqlite')



# total sold 
dbGetQuery(con, "select p.pid,p.pname ,sum(s.quantity) as total_sold
           from salestxn s 
           join products p on p.pid = s.pidFK
           group by pname")

# total sold by each quarter and year 
dbGetQuery(con , "SELECT
    p.pid,
    p.pname,
    STRFTIME('%Y', st.date) AS year,
    (CAST(STRFTIME('%m', st.date) AS INTEGER) - 1) / 3 + 1 AS quarter,
    SUM(st.quantity) AS total_sold_per_quarter_and_year
FROM products p
JOIN salestxn st ON p.pid = st.pidFK
GROUP BY p.pid, p.pname, year, quarter;
")


dbGetQuery(con, "
SELECT
p.pid,
p.pname,
r.region,
SUM(st.quantity) AS total_sold_per_region
FROM products p
JOIN salestxn st ON p.pid = st.pidFK
JOIN reps r ON st.ridFK = r.rid
GROUP BY p.pid, p.pname, r.region;")

##################################################################

dbGetQuery(con , "SELECT
    total_sold.pid,
    total_sold.pname,
    total_sold.total_sold,
    quarter_and_year.year,
    quarter_and_year.quarter,
    region.region,
    region.total_sold_per_region
FROM (
    -- Total sold for each product
    SELECT
        p.pid,
        p.pname,
        SUM(s.quantity) AS total_sold
    FROM salestxn s 
    JOIN products p ON p.pid = s.pidFK
    GROUP BY p.pid, p.pname
) total_sold
LEFT JOIN (
    -- Total sold by each quarter and year
    SELECT
        p.pid,
        STRFTIME('%Y', st.date) AS year,
        (CAST(STRFTIME('%m', st.date) AS INTEGER) - 1) / 3 + 1 AS quarter,
        SUM(st.quantity) AS total_sold_per_quarter_and_year
    FROM products p
    JOIN salestxn st ON p.pid = st.pidFK
    GROUP BY p.pid, year, quarter
) quarter_and_year ON total_sold.pid = quarter_and_year.pid
LEFT JOIN (
    -- Total sold per region
    SELECT
        p.pid,
        r.region,
        SUM(st.quantity) AS total_sold_per_region
    FROM products p
    JOIN salestxn st ON p.pid = st.pidFK
    JOIN reps r ON st.ridFK = r.rid
    GROUP BY p.pid, r.region
) region ON total_sold.pid = region.pid;
")

############################################################################################


# sales rep fact table
dbGetQuery( con, "SELECT
r.rid,
r.fname,
r.lname,
SUM(st.quantity) AS total_sold
FROM reps r
JOIN salestxn st ON r.rid = st.ridFK
GROUP BY r.rid, r.fname, r.lname;")



dbGetQuery(con, "SELECT
    r.rid,
    r.fname,
    r.lname,
    STRFTIME('%Y', st.date) AS year,
    (CAST(STRFTIME('%m', st.date) AS INTEGER) - 1) / 3 + 1 AS quarter,
    SUM(st.quantity) AS total_sold_per_quarter_and_year
FROM reps r
JOIN salestxn st ON r.rid = st.ridFK
GROUP BY r.rid, r.fname, r.lname, year, quarter;")




dbGetQuery(con , "SELECT
r.rid,
r.fname,
r.lname,
p.pname,
SUM(st.quantity) AS total_sold_per_product
FROM reps r
JOIN salestxn st ON r.rid = st.ridFK
JOIN products p ON st.pidFK = p.pid
GROUP BY r.rid, r.fname, r.lname, p.pname;")


# combining all the tables 

dbGetQuery(con , "SELECT
    rep_total.rid,
    rep_total.fname,
    rep_total.lname,
    rep_total.total_sold,
    quarter_and_year.year,
    quarter_and_year.quarter,
    product_total.pname,
    product_total.total_sold_per_product
FROM (
    -- Step 1: Total quantity sold for each sales rep
    SELECT
        r.rid,
        r.fname,
        r.lname,
        SUM(st.quantity) AS total_sold
    FROM reps r
    JOIN salestxn st ON r.rid = st.ridFK
    GROUP BY r.rid, r.fname, r.lname
) rep_total
LEFT JOIN (
    -- Step 2: Total quantity sold per year and quarter for each sales rep
    SELECT
        r.rid,
        STRFTIME('%Y', st.date) AS year,
        (CAST(STRFTIME('%m', st.date) AS INTEGER) - 1) / 3 + 1 AS quarter,
        SUM(st.quantity) AS total_sold_per_quarter_and_year
    FROM reps r
    JOIN salestxn st ON r.rid = st.ridFK
    GROUP BY r.rid, year, quarter
) quarter_and_year ON rep_total.rid = quarter_and_year.rid
LEFT JOIN (
    -- Step 3: Total quantity sold per product for each sales rep
    SELECT
        r.rid,
        p.pname,
        SUM(st.quantity) AS total_sold_per_product
    FROM reps r
    JOIN salestxn st ON r.rid = st.ridFK
    JOIN products p ON st.pidFK = p.pid
    GROUP BY r.rid, p.pname
) product_total ON rep_total.rid = product_total.rid;
")



#####################################################################################################
query <-  dbGetQuery(con,"SELECT
p.pname,
STRFTIME('%Y-%m', st.date) AS month,
SUM(st.quantity) AS total_sold_per_month
FROM products p
JOIN salestxn st ON p.pid = st.pidFK
GROUP BY p.pname, month;")


dbExecute(con, "drop table if exists monthly_sales;")
dbExecute(con, "CREATE TABLE monthly_sales (
    pname TEXT,
    month DATE,
    total_sold_per_month INT
);")


dbWriteTable(con, "monthly_sales", query, append=TRUE)

library(ggplot2)

query <- "select * from monthly_sales"
data <- dbGetQuery(con, query)
data

ggplot(data, aes(x = month, y = total_sold_per_month, group = pname, color = pname)) +
  geom_line() +
  geom_point() +
  labs(title = "Total Sold per Month",
       x = "Month",
       y = "Total Sold") +
  theme_minimal() +
  theme(axis.text.x = element_text(angle = 45, hjust = 1))
#######################################################################################

dbDisconnect(con)
