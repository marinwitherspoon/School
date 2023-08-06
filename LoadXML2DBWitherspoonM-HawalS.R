# PART 1-  LOAD XML DATA

# Authors : Marin Witherspoon and Sumit Hawal
# Course  : CS5200 DBMS SuF
# Date    : 02/08/2023


# loading required packages
library('RSQLite')
library(XML)


# Getting the database name 
data <- 'database.sqlite'

# creating a sqlite connection
con <- dbConnect(RSQLite::SQLite(),data)


# drop the tables if exists -- (to avoid getting errors while running the scripts) i
dbExecute(con, "drop table if exists products;")
dbExecute(con, "drop table if exists customers;")
dbExecute(con, "drop table if exists reps;")
dbExecute(con, "drop table if exists salestxn;")
dbExecute(con, "drop table if exists cust_reps")



# Create Products table
dbExecute(con,
          "CREATE TABLE products (
          pid INT PRIMARY KEY ,
          pname TEXT NOT NULL
          )")


# creating Reps table
dbExecute(con, "
          CREATE TABLE reps ( 
          rid INT PRIMARY KEY,
          fname TEXT,
          lname TEXT,
          region TEXT)")


# creating Customers table 
dbExecute(con,"CREATE TABLE customers (
          cid INT PRIMARY KEY,
          customer TEXT,
          country TEXT
)")

# creating Sales table 
dbExecute(con, "CREATE TABLE salestxn (
          tid INT PRIMARY KEY,
          date DATE,
          quantity INT,
          amount INT,
          ridFK INT NOT NULL,
          pidFK INT NOT NULL,
          cidFK INT NOT NULL,
          FOREIGN KEY (ridFK) REFERENCES reps (rid),
          FOREIGN KEY (pidFK) REFERENCES products (pid),
          FOREIGN KEY (cidFK) REFERENCES customers (cid)
)")


# creating a customer and representative relationship for one to many relationships 
dbExecute(con , "
          CREATE TABLE cust_reps ( 
          cid INT ,
          rid INT, 
          PRIMARY KEY (cid,rid),
          FOREIGN KEY (cid) REFERENCES customers(cid),
          FOREIGN KEY (rid) REFERENCES reps(rid))")

################################################################################################
folder_path <- "txn-xml"

# processing the Representative file
reps_data <- function(path) { 
  
  reps_xml <- xmlParse(path)   # reading the file from path
  reps <- xmlToDataFrame(nodes=getNodeSet(reps_xml , "//rep"))   # making the dataframe from the xml data
  
  # getting the nodes from data
  nodes <- getNodeSet(reps_xml, "//rep")
  
  # making a list to save the representatives 
  rid_list <- list()
  for ( node in nodes){
    id <- xmlAttrs(node)[["rID"]]
    rid_list <- c(rid_list,id)
  }
  
  # making the rid into integer by removing r and converting to integer
  rid_index <- as.integer(sub("r", "", unlist(rid_list)))
  
  # giving the index to rid column
  reps$rid <- rid_index
  
  # rearranging the columns such that rid becomes first
  reps <- reps[, c("rid", "firstName", "lastName", "territory")]
  
  #renaming the columns according to table definitions
  colnames(reps) <- c("rid", "fname", "lname", "region")
  
  # writing the dataframe to our sqlite entity
  dbWriteTable(con, "reps", reps, overwrite = TRUE)
  
}
# as this folder is expected we don't write any explicit code for this
reps_data("txn-xml\\pharmaReps.xml")

# checking if data successfully was inserted
dbGetQuery(con, "select * from reps")



####################################################################################################

#function to populate products table
process_products_data <- function(folder_path){

# Get a list of all XML files with the pattern "pharmaSale*.xml" in the folder
xml_files <- list.files(path = folder_path, pattern = "pharmaSale.*\\.xml", full.names = TRUE)


# Initialize an empty dataframe to store the combined data
combined_data <- data.frame()

# Loop through each XML file and process the data
for (file_path in xml_files) {
  sales_xml <- xmlParse(file_path)
  products <- xmlToDataFrame(nodes = getNodeSet(sales_xml, "//prod"))
  
  # Select only distinct products by "pname"
  unique_products <- unique(products, by = "prod")
  
  # Append the unique products to the combined_data dataframe
  combined_data <- rbind(combined_data, unique_products)

}

# Remove duplicates and keep only unique pname
combined_data <- unique(combined_data)

# Generate unique pid as it is synthetic 
combined_data$pid <- seq_len(nrow(combined_data))

print(combined_data)
combined_data <- combined_data[, c("pid", "text")]
colnames(combined_data) <- c('pid','pname') 
print(combined_data)
return(combined_data)

}

# change the folder path according to your system

products_data <- process_products_data(folder_path)
print(products_data)
# writing the dataframe to table
dbWriteTable(con, "products", products_data, append = TRUE)


########################################################################################################


# Populating the customers table
process_customer_data <- function(path){
# Get a list of all XML files with the pattern "pharmaSale*.xml" in the folder
xml_files <- list.files(path = folder_path, pattern = "pharmaSale.*\\.xml", full.names = TRUE)

# Initialize an empty dataframe to store the combined data
combined_cust_data <- data.frame()

# Loop through each XML file and process the data
for (file_path in xml_files) {
  sales_xml <- xmlParse(file_path)
  customer <- xmlToDataFrame(nodes = getNodeSet(sales_xml, "//txn"))

  # getting rid of duplicate enteries of customers  within the xml files
  unique_customer <- customer[!duplicated(customer$cust), ]
  # Append the unique customer  to the combined_data dataframe
  combined_cust_data <- rbind(combined_cust_data, unique_customer)   } 
    
# dropping duplicate rows accumulated throughout the files
combined_cust_data <- combined_cust_data[!duplicated(combined_cust_data$cust),]

# creating synthetic key cid for our table
combined_cust_data$cid <- 100 + seq_len(nrow(combined_cust_data))

# rearranging the rows 
combined_cust_data <- combined_cust_data[, c("cid", "cust","country")]
# changing the column names according to our table definitions
colnames(combined_cust_data) <- c('cid','customer',"country") 

return(combined_cust_data)
}


  
customer_data <- process_customer_data(folder_path)

# writing the table to our sqlite entity
dbWriteTable(con, "customers", customer_data, append = TRUE)




#################################################################################################
# creating the customer relation table  - for many to many relationship between tables 


process_cst_data <- function(con, folder_path) {
  
  # Get a list of all XML files with the pattern "pharmaSale*.xml" in the folder
  xml_files <- list.files(path = folder_path, pattern = "pharmaSale.*\\.xml", full.names = TRUE)
  
  # Initialize an empty dataframe to store the combined data
  combined_cst_data <- data.frame(cid = integer(), rid = integer())
  
  # traversing all the xml_files
  for (file_path in xml_files) {
    sales_xml <- xmlParse(file_path)
    # creating a dataframe of our current xml files
    transactions <- xmlToDataFrame(nodes = getNodeSet(sales_xml, "//txn"))
    
    for (i in seq_len(nrow(transactions))) {
      # Extract the customer name and repID for each transaction
      customer_name <- transactions[i, "cust"]
      rep_id <- transactions[i, "repID"]
      
      # Get the customer ID (cid) from the "customers" table based on the customer name
      cid <- dbGetQuery(con, sprintf("SELECT cid FROM customers WHERE customer = '%s'", customer_name))$cid
      
      # Get the representative ID (rid) from the "reps" table based on the repID
      rid <- dbGetQuery(con, sprintf("SELECT rid FROM reps WHERE rid = '%s'", rep_id))$rid
      
      # Append the unique (cid, rid) if only it does not exists in our table
      if (!any(combined_cst_data$cid == cid & combined_cst_data$rid == rid)) {
        combined_cst_data <- rbind(combined_cst_data, data.frame(cid = cid, rid = rid))
      }
    }
  }
  
  
  # Now, create a new data frame with column names of our sqlite table
  cust_reps_data <- data.frame(
    cid = combined_cst_data$cid,
    rid = combined_cst_data$rid
  )
  
  dbWriteTable(con, "cust_reps", cust_reps_data, row.names = FALSE,overwrite=TRUE)
}


process_cst_data(con, folder_path)

# checking if we successfull added the data
dbGetQuery(con, "select * from cust_reps")
#####################################################################################################


# populating the sales tables
process_sales_data <- function(con, folder_path) {
  # Get a list of all XML files with the pattern "pharmaSale*.xml" in the folder
  xml_files <- list.files(path = folder_path, pattern = "pharmaSale.*\\.xml", full.names = TRUE)
  
  # Initialize an empty list to store individual dataframes from each XML file
  salestxn_data_list <- list()
  
  
  # tid 
  last_tid <- 0
  for (file_path in xml_files) {
    sales_xml <- xmlParse(file_path)
    # creating a dataframe from current dataframe
    transactions <- xmlToDataFrame(nodes = getNodeSet(sales_xml, "//txn"))
    
    # Process each row and extract data we require 
    for (i in seq_len(nrow(transactions))) {
      txn_date <- transactions[i, "date"]
      quantity <- transactions[i, "qty"]
      amount <- transactions[i, "amount"]
      customer_name <- transactions[i, "cust"]
      product_name <- transactions[i, "prod"]
      rep_id <- transactions[i, "repID"]
      
      # matching the keys based on the customer_name product_name and rep_id from our tables
      cid <- dbGetQuery(con, sprintf("SELECT cid FROM customers WHERE customer = '%s'", customer_name))$cid
  
      pid <- dbGetQuery(con, sprintf("SELECT pid FROM products WHERE pname = '%s'", product_name))$pid
    
      rid <- dbGetQuery(con, sprintf("SELECT rid FROM reps WHERE rid = '%s'", rep_id))$rid

      # This the encoding schema for converting the date
      # first it is extracted as string and then using as.Date it is converted to data according to this schema(e.g., YYYY-MM-DD)
      # Convert the date
      converted_date <- as.Date(txn_date, format = "%m/%d/%Y")
      
      # tid append
      last_tid <- last_tid + 1
      
      # Append the transaction data to the list
      salestxn_data_list[[length(salestxn_data_list) + 1]] <- data.frame(
        tid = last_tid,
        date = converted_date,
        quantity = as.integer(quantity),
        amount = as.integer(amount),
        rid = rid,
        pid = pid,
        cid = cid
      )
    }
  }
  
  # Combine add each list into a single dataframe
  combined_salestxn_data <- do.call(rbind, salestxn_data_list)
  
  # Now, create a new data frame with column names according to our table columns
  salestxn_data <- data.frame(
    tid = combined_salestxn_data$tid,
    date = combined_salestxn_data$date,
    quantity = combined_salestxn_data$quantity,
    amount = combined_salestxn_data$amount,
    ridFK = combined_salestxn_data$rid,
    pidFK = combined_salestxn_data$pid,
    cidFK = combined_salestxn_data$cid
  )
  
  # returning the final created dataframe
  return(salestxn_data)
}


data <- process_sales_data(con,folder_path)

data

for (i in seq_len(nrow(data))) {
  dbExecute(con, sprintf("INSERT INTO salestxn (tid, date, quantity, amount, ridFK, pidFK, cidFK) VALUES (%d, '%s', %d, %d, %d, %d, %d)",
                         data$tid[i], data$date[i], data$quantity[i], data$amount[i], data$ridFK[i], data$pidFK[i], data$cidFK[i]))
}

dbGetQuery(con, "select count(*) from salestxn ")

dbGetQuery(con, "pragma table_info(salestxn)")
# This the encoding schema for converting the date
# first it is extracted as string and then using as.Date it is converted to data according to this schema(e.g., YYYY-MM-DD)


#####################################################################################################

# disconnect the database
dbDisconnect(con)
