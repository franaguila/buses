#!/usr/bin/Rscript


library(RODBC)
library(dplyr)
library(ggplot2)
library(cluster)
library(knitr)
library(reshape2)

con_str <- paste0("Driver={SQL Server};Server=Savona;Database=service_analysis;trusted_connection=true")  
connection <- odbcDriverConnect(con_str)

#read in the unique from and to stops in the data we have (used to be tp_data_v2)
Q <- paste0("SELECT * FROM runtime_analysis.unique_timing_point_pairs")
data0 <- sqlQuery(connection, Q)

#now stack them into one column of unique stops
data1 <- unique(melt(data0) %>% select(stop_no = value))

#read in information about each stop in CMBC's system
#Q2 <- paste0("SELECT * FROM runtime_analysis.stop_locations where from_stop_no is not null")

Q2 <- paste0("select stop_no as from_stop_no, min(stop_latitude) as stop_lat, min(stop_longitude) as stop_lon, min(node_code) as timing_point 
             from shardim.dim_transit_point 
             where dw_current_flag = 'Y' and stop_latitude <> 49 and stop_longitude <> -123 and transit_point_type = 'node' and node_code <> '-1' Group by stop_no")

data2 <- sqlQuery(connection, Q2)

#this is every bus stop seen in the data and its lon/lat
data3 <- data1 %>% left_join(data2 %>% select(from_stop_no, stop_lon, stop_lat, timing_point), by = c("stop_no" = "from_stop_no"))

#remove the ones we don't know lon/lat for
data <- data3[complete.cases(data3),] %>% arrange(stop_lon, stop_lat)

#cluster the stops, I tweaked this number until this had nearly zero rows: clustered_data %>% filter(from_stop_group == to_stop_group)View
model <- pam(data[ ,2:3],250)
clusters <- model$clustering

#append the clusters to the dataframe
data$stop_group <- clusters

clustered_data <- data0 %>% 
  left_join(data %>% select(stop_no, stop_group, from_stop_lon = stop_lon, from_stop_lat = stop_lat, from_timing_point = timing_point), by = c("from_stop_no" = "stop_no")) %>% 
  rename(from_stop_group = stop_group) %>% 
  left_join(data %>% select(stop_no, stop_group, to_stop_lon = stop_lon, to_stop_lat = stop_lat, to_timing_point = timing_point), by = c("to_stop_no" = "stop_no")) %>% 
  rename(to_stop_group = stop_group)

#upload to the azure DB
var_types <- c(stop_no = "VARCHAR(255)", stop_lon = 'FLOAT(24)', stop_lat = 'FLOAT(24)', stop_group = "VARCHAR(255)")

sqlQuery_truncate <- "Truncate Table runtime_analysis.clustered_stops"
sqlQuery_truncateEXE <- sqlQuery(connection, sqlQuery_truncate)
sqlSave(channel = connection, dat = data, tablename = "runtime_analysis.clustered_stops", rownames = FALSE, append = TRUE)


print("Yay cluster.R worked")