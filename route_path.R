#!/usr/bin/Rscript

############
# Finding Route paths
# 
# File Dependencies:
#   File: data/google_transit/routes.txt
#   From: 
#
# RMD version has some other code for visual verification
#
############



# Libraries
############
library(tidyverse)
library(curl)
library(xml2)
library(sp)
library(geoR)
library(stringr)
library(ggmap)
library(RODBC)


# Dependencies
############

route_nums <- read_csv("./data/google_transit/routes.txt") %>% 
  select(route_short_name)


# Download all shape files 
############
download_kmz_files <- function(a_link) {
  download_name <- str_split(a_link,"trip/")[[1]][2]
  download.file(a_link, destfile = paste0("./data/routes/",download_name))
}


get_route_info <- function(a_route_no) {
  API_KEY <- "QEeo8QPDtgwmoFZulLvF"
  dump <- suppressWarnings(curl(paste0("http://api.translink.ca/rttiapi/v1/routes/",a_route_no,"?apikey=",API_KEY)))
  xml <- read_xml(readLines(dump))
  
  # Get all kmz files for the line patterns we have for a route.
  kmz_links <- xml_text(xml_find_all(xml, ".//Href"))
  
  map(.x = kmz_links, .f=download_kmz_files)
  print(paste0("done: ",a_route_no))
}




# Parallaized loop over all routes that work
############
suppressMessages(suppressWarnings(map(.x = route_nums$route_short_name[197:nrow(route_nums) ], .f = get_route_info)))


# Zips all files cause weird things you have to do to extract information
############
data_location = "./data/routes/"
files_to_edit <- list.files(pattern = "*.kmz", path=data_location)


convert_to_zip <- function(a_file) {
  
  new_name <- paste0(data_location, str_split(a_file, "\\.")[[1]][1], ".zip")
  file.rename(paste0(data_location,a_file), new_name)
}

map(.x= files_to_edit, .f = convert_to_zip)




# Parses all ZIP files 
############

master_routes <- list()
route_names <- c()

setwd(paste0(original_wd,"/./data/routes"))

zips_to_parse <- list.files(pattern = "*.zip")

sum = 0
for (a_zip in zips_to_parse) {
  
  unzip(a_zip)
  an_xml <- read_xml("./doc.kml")
  
  xname <- xml_text(xml_find_first(an_xml, "./*/*/*[1]"))
  coords <- xml_text(xml_find_all(an_xml, "/*/*/*[3]/*[2]/*[4]/*"))
  
  split_coords <- str_split(coords, ",| ", simplify = TRUE)
  
  route <- tibble(
    from_stop_lon = as.numeric(split_coords[,1]),
    from_stop_lat = as.numeric(split_coords[,2]),
    to_stop_lon = as.numeric(split_coords[,4]),
    to_stop_lat = as.numeric(split_coords[,5])
  )
  
  route_names <- c(route_names, xname)
  master_routes <- append(master_routes, list(xname = route))
  
  sum = sum + 1
}

names(master_routes) <-  route_names


setwd(original_wd)


# Creates useful columns from compact data
############

route_names <- names(master_routes)

# Add route-patern into dataframe so it can be merged with other dfs and used.
for (df_num in 1:length(master_routes)) {
  master_routes[[df_num]]$route_pattern <- rep(route_names[df_num], nrow(master_routes[[df_num]]))
}  

# split line-pattern and split into columns
for (df_num in 1:length(master_routes)) {
  
  df_length <- nrow(master_routes[[df_num]])
  
  split_line_pattern = str_split(master_routes[[df_num]]$route_pattern[1], "-")[[1]]
  split_direction = str_split(split_line_pattern[2], "B")[[1]][1]
  
  direction_to_put <- ""
  if (split_direction == "N") {
    direction_to_put = "NORTH"
  } else if (split_direction == "S") {
    direction_to_put = "SOUTH"
  } else if (split_direction == "E") {
    direction_to_put = "EAST"
  } else if (split_direction == "W") {
    direction_to_put = "WEST"
  }
  
  master_routes[[df_num]]$route <- rep(split_line_pattern[1], df_length)
  master_routes[[df_num]]$pattern <- rep(split_line_pattern[2], df_length)
  master_routes[[df_num]]$direction_name <- rep(direction_to_put, df_length)
}  


# Convert short direction names to long names
############

for (df_num in 1:length(master_routes)) {
  
  pattern_for_df <- master_routes[[df_num]]$pattern[1]
  short_direction <- substring(pattern_for_df,1,1)
  
  direction_to_put <- ""
  
  if (short_direction == "N") {
    direction_to_put = "NORTH"
  } else if (short_direction == "S") {
    direction_to_put = "SOUTH"
  } else if (short_direction == "E") {
    direction_to_put = "EAST"
  } else if (short_direction == "W") {
    direction_to_put = "WEST"
  }
  
  master_routes[[df_num]]$direction_name <- direction_to_put
}

combined_routes <- bind_rows(master_routes)



# Save to SQL
############

# write_csv(combined_routes,path = )

con_str <- paste0("Driver={SQL Server};Server=Savona;Database=service_analysis;trusted_connection=true")  

connection <- odbcDriverConnect(con_str)

#sqlDrop(channel=connection, sqtable = "runtime_analysis.route_coords_test")
sqlSave(channel=connection, dat = combined_routes, tablename = "runtime_analysis.route_coords_test", rownames = FALSE )
