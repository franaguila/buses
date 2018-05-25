#!/usr/bin/Rscript

############
# Grouping Route Coordinates
# 
# SQL Dependencies:
#   SQL: route_coords_small 
#   SQL: tp_with_latlon
#
# RMD version has some other code for visual verification
#
############

# Libraries
############
library(fuzzyjoin)
library(stringr)
library(tidyverse)
library(RODBC)

# SQL Connection and pulling
############

tp_query <- "SELECT * from [runtime_analysis].[tp_with_latlon]"
tp_with_latlon <- sqlQuery(channel = connection, tp_query)


route_coords_query <- "SELECT * FROM [runtime_analysis].[route_coords_small]"
routes <- sqlQuery(connection, route_coords_query)

# Find next and previous timing points in proper order
############

# Save the order of the routes explicitely as a column because R does not guaruntee order
routes_new <- routes
routes_new$order <- seq(1,nrow(routes))

# Create timing point pairs rather than just timing points as we are interested in edges not vertexes 
tp_joined_prepped <- tp_with_latlon %>% 
  mutate(from_timing_point = as.character(timing_point)) %>% 
  group_by(line_no, direction_name) %>% 
  mutate(next_tp = lead(timing_point),
         past_tp = lag(timing_point)) %>% 
  mutate(LR = paste0(timing_point, "-",next_tp),
         RL = paste0(timing_point, "-",past_tp))




# Get line/direction combos to iterate on
############

line_direction_combo <- tp_joined_prepped %>% 
  mutate(combo = paste0(line_no, "-", direction_name)) %>% 
  distinct(combo) %>% 
  arrange(line_no)

line_direction_combo <- line_direction_combo$combo



# Parallalized for loop over all line/direction combos
############

solver <- function(x) {
  
  tryCatch({
    split_line_direction <- unlist(str_split(x, pattern = "-"))
    
    tp_filtered <- tp_joined_prepped %>%
      filter(line_no == split_line_direction[1],
             direction_name == split_line_direction[2])
    
    # Using only 1 direction's route for a line/route
    routes_filtered <- routes_new %>% 
      filter(route == split_line_direction[1])
    
    routes_filtered <- routes_filtered[order(routes_filtered$route, routes_filtered$order),]
    
    colours_froms <- geo_join(tp_filtered, 
                              routes_filtered, 
                              by = c("stop_lon" = "from_stop_lon", 
                                     "stop_lat" = "from_stop_lat"), 
                              max_dist = 16,  
                              method = "cosine", 
                              mode= "full", 
                              distance_col = "dist")
    
    colours_froms_red <- colours_froms %>% 
      group_by(from_stop_lat, from_stop_lon) %>% 
      slice( which.min(dist) )
    
  }, error=function(error_message) {
    message("Something screwed up ", x)
  }
  )
  
  
  
}

test_coord <- bind_rows(map(.x = line_direction_combo, .f = solver))


# Clean up of the earlier join
############

route_coords_tpp <- test_coord %>% 
  rename(direction_name = direction_name.x) %>%
  group_by(line_no, direction_name) %>% 
  arrange(order) %>% 
  select(line_no, timing_point, direction_name, next_tp, past_tp, LR, RL, from_stop_lat, from_stop_lon, to_stop_lat, to_stop_lon, route_pattern, pattern, order)



# Gathering timing point information into a long table format
############

route_coords_tpp_eh <- route_coords_tpp %>% 
  gather( "right_or_left", "to_tp", next_tp,past_tp) %>% 
  mutate(tp_pair = paste0(timing_point, "-",to_tp)) %>% 
  drop_na(to_tp)

route_coords_tpp_eh$pk = seq(1,nrow(route_coords_tpp_eh))


# SQL upload (assuming table already exists we drop it)
############
sqlDrop(channel=connection, sqtable = "runtime_analysis.route_coords_tpp")
sqlSave(channel=connection, dat = route_coords_tpp_eh, tablename = "runtime_analysis.route_coords_tpp", rownames = FALSE )

print("Yay grouping_route_coords.R worked")
