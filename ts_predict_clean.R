# loading packages
library(RODBC)
library(tidyverse)
library(lubridate)
library(purrr)

# SQL query for all the data
sqlQuery_view <- "SELECT tp.operating_date, tp.sheet_code, tp.line_no, tp.direction_name,
tp.sch_min_interval, tp.sch_max_interval,
CONCAT(tp.from_stop_group, '-', tp.to_stop_group) AS tpp_group, tp.from_timing_point, tp.to_timing_point,
CONCAT(tp.from_timing_point, '-', tp.to_timing_point) AS tpp,
tp.from_point_desc + ' to ' + tp.to_point_desc + ' (' + tp.from_timing_point + '-' + tp.to_timing_point + ')' as tpp_desc,
tp.service_type_code, tp.from_sch_leave_time, tp.from_act_leave_time, tp.act_run_time,
tp.sch_run_time, tp.from_act_leave_datetime, getdate() as run_time_stamp
FROM runtime_analysis.tp_data tp
where depot_code in ('BTC', 'HTC', 'NVT', 'PCT', 'RTC', 'STC', 'VTC', 'XHT', 'XNE', 'XOT', 'XSS')"

# connect to Savona db
data <- sqlQuery(connection, sqlQuery_view)

# this is a function that takes in a baseline data frame, 
# baseline and prediction sheet codes, and week numbers of the first and last weeks
# and generates a new data frame for the desired scheduling period
generate_predict_sheet <- function(dataframe, baseline_sheet_code, predict_sheet_code="17SEP", first_week, last_week){
  
  # we only want the baseline sheet code and unique time segments (no duplicates)
  baseline_df <- dataframe %>% 
    filter(sheet_code == baseline_sheet_code) %>% 
    select(sch_min_interval, sch_max_interval, line_no, from_timing_point, to_timing_point, tpp_group, tpp_desc, direction_name, service_type_code, sheet_code, run_time_stamp) %>% 
    unique()
  
  # generate the week numbers we need
  week_list <- c(first_week:last_week)
  no_weeks <- length(week_list)
  no_row_df <- nrow(baseline_df)
  weeks <- rep(first_week:last_week, each = no_row_df)
  baseline_df_week <- do.call("rbind", replicate(no_weeks, baseline_df, simplify = FALSE))
  
  # generate day_sec for regression later on
  baseline_df_week_daysec <- baseline_df_week %>% 
    mutate(week = as.numeric(weeks), 
           sheet_code = predict_sheet_code,
           day_sec = (sch_min_interval + sch_max_interval)/2)
  
  return(baseline_df_week_daysec) # final data frame for schedule!
  
}


# this is a function that takes in a data frame to run our regression on, a data frame to predict on, and the prediction year
# passes the data into another function to predict run times for the second data frame
# and returns a final data frame containing model predictions for run time
predict_schedule <- function(dataframe, df_to_predict, predict_year){
  
  # create a list of lines for the for loop
  line_list <- levels(factor(dataframe$line_no))
  # initialize an empty data frame
  predictions_dfs <- data.frame(sch_min_interval = numeric(0), sch_max_interval = numeric(0), 
                                prediction = numeric(0))
  
  # loop over all the lines
  for(line in line_list){
    
    print(paste0("line:", line))
    train1 <- dataframe %>% 
      filter(line_no == line) %>% 
      mutate(year = year(from_act_leave_datetime),
             week = week(from_act_leave_datetime),
             weekyear = year + (2*week-1)/(52*2),  # create a value "weekyear" to create a continuous time axis
             day_sec = from_act_leave_time)
    
    # change the weekyear to be next year's
    test1 <- df_to_predict %>% 
      filter(line_no == line) %>% 
      mutate(year = as.numeric(predict_year),
             weekyear = year + (2*week-1)/(52*2))
    
    # make sure we have data for both train and test
    if(nrow(train1) == 0 | nrow(test1) == 0){
    } else {
      
      # create list of timing point group pairs
      tpp_list <- levels(factor(train1$tpp_group))
      
      # call the next function that contains the model
      predictions_df <- map(tpp_list, decomposition_model, train = train1, test = test1)
      
      # combine all the timing point group pair data for this specific line
      predictions_df <- bind_rows(bind_rows(predictions_df))
      
      # conditional to makes sure there's data before merging
      if(nrow(predictions_df) == 0){
      } else {
        
        # combine all the bus lines
        predictions_dfs <- full_join(predictions_df, predictions_dfs)
        
      }
    }
  }
  
  return(predictions_dfs)
  
}

# this function takes in a timing point group, training data, and test data
# trains the trend decomposition model on the training data and generates predictions on test data
# this function returns a dataframe with predictions for a specific timing point group
decomposition_model <- function(tpgroup, train, test){
  
  print(paste0("specific tpp:", tpgroup))
  
  # subset for the particular timing point group
  # we need to run model on each timing point group
  train <- train %>% 
    filter(tpp_group == tpgroup)
  test <- test %>% 
    filter(tpp_group == tpgroup)
  
  # aggregating data by week before running linear model
  trend_df <- train %>% 
    group_by(year, week) %>% 
    summarise(act_run_time = mean(act_run_time, na.rm = TRUE),
              weekyear = unique(weekyear))
  
  # linear model that estimates the overall trend
  linear_trend <- lm(act_run_time ~ weekyear, data = trend_df)
  
  # predict on train and test data
  train$y_trend <- predict(linear_trend, train)
  test$yhat_trend <- predict(linear_trend, test)
  
  # subtract/decompose the trend
  train$y_trendless <- train$act_run_time - train$y_trend
  
  # prepare a table to use for estimating the yearly seasonal component
  year_seasonal_df <- train %>% 
    group_by(year, week) %>% 
    summarise(y_trendless = mean(y_trendless, na.rm = TRUE))
  
  tryCatch({    # error handling in case data too little for loess
    
    # estimating yearly seasonal 1
    
    # local regression that estimates yearly seasonal component
    loess_year_seasonal <- loess(y_trendless ~ week,
                                 data = year_seasonal_df,
                                 span = 0.35, 
                                 degree = 2)
    
    # predict on train and test data
    train$y_year_seasonal <- predict(loess_year_seasonal, train)
    test$yhat_year_seasonal <- predict(loess_year_seasonal, test$week)
    
  }, error=function(e){
    
    # estimating yearly seasonal 2
    
    tryCatch({    # error handling in case data too little for loess
      
      # local regression that estimates yearly seasonal component
      loess_year_seasonal <- loess(y_trendless ~ week,
                                   data = year_seasonal_df,
                                   span = 1, 
                                   degree = 0)
      
      # predict on train and test data
      train$y_year_seasonal <- predict(loess_year_seasonal, train)
      test$yhat_year_seasonal <- predict(loess_year_seasonal, test$week)
      
    }, error=function(e){
      
      print(paste0("There may not be enough data for line ", train$line_no[1], " timing point pair ", tpgroup, " to calculate yearly seasonal component"))
      
    })
    
  })
  
  tryCatch({    # error handling in case data too little for calculation
    
    # subtract/decompose the yearly seasonal component
    train$y_trendless_yearseasonless <- train$y_trendless - train$y_year_seasonal
   
  }, error=function(e){
    
    print(paste0("There may not be enough data for line ", train$line_no[1], " timing point pair ", tpgroup, " to calculate yearly seasonal component"))
    
  })
  
  # dividing data by day type in preparation for local regression on each
  
  # Saturday
  train_sat <- train %>% 
    filter(service_type_code == "SAT")
  test_sat <- test %>% 
    filter(service_type_code == "SAT")
  
  tryCatch({    # error handling in case data too little for loess
    
    # generate a loess model for saturday
    loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                     data = train_sat,
                                     span = 0.20, 
                                     degree = 0,
                                     control=loess.control(surface="direct"))
    
    # predict on train and test data
    train_sat$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_sat)
    test_sat$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_sat)
    
  }, error=function(e){
    
    tryCatch({    # error handling in case data too little for loess
      
      # generate a loess model for saturday
      loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                       data = train_sat,
                                       span = 1, 
                                       degree = 0,
                                       control=loess.control(surface="direct"))
      
      # predict on train and test data
      train_sat$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_sat)
      test_sat$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_sat)
      
    }, error=function(e){
      
      print(paste0("There may not be enough data for line ", train$line_no[1], " timing point pair ", tpgroup, " to calculate Saturday component"))
      
    })
    
  })
  
  # Sunday
  train_sun <- train %>% 
    filter(service_type_code == "SUN")
  test_sun <- test %>% 
    filter(service_type_code == "SUN")
  
  tryCatch({
    
    # generate a loess model for sunday
    loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                     data = train_sun,
                                     span = 0.20, 
                                     degree = 0,
                                     control=loess.control(surface="direct"))
    
    # predict on train and test data
    train_sun$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_sun)
    test_sun$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_sun)
    
  }, error=function(e){
    
    tryCatch({
      
      # generate a loess model for sunday
      loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                       data = train_sun,
                                       span = 1, 
                                       degree = 0,
                                       control=loess.control(surface="direct"))
      
      # predict on train and test data
      train_sun$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_sun)
      test_sun$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_sun)
      
    }, error=function(e){
      
      print(paste0("There may not be enough data for line ", train$line_no[1], " timing point pair ", tpgroup, " to calculate Sunday component"))
      
    })
    
  })
  
  # M-F
  train_weekday <- train %>% 
    filter(service_type_code == "MF")
  test_weekday <- test %>% 
    filter(service_type_code == "MF")
  
  tryCatch({
    
    # generate a loess model for weekdays
    loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                     data = train_weekday,
                                     span = 0.20,
                                     degree = 1,
                                     control=loess.control(surface="direct"))
    
    # predict on train and test data
    train_weekday$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_weekday)
    test_weekday$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_weekday)
    
  }, error=function(e){
    
    tryCatch({
      
      # generate a loess model for weekdays
      loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                       data = train_weekday,
                                       span = 10,
                                       degree = 0,
                                       control=loess.control(surface="direct"))
      
      # predict on train and test data
      train_weekday$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_weekday)
      test_weekday$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_weekday)
      
    }, error=function(e){
      
      print(paste0("There may not be enough data for line ", train$line_no[1], " timing point pair ", tpgroup, " to calculate weekday component"))
      
    })
    
  })
  
  # XMS
  train_XMS <- train %>% 
    filter(service_type_code == "XMS")
  test_XMS <- test %>% 
    filter(service_type_code == "XMS")
  
  tryCatch({
    
    # generate a loess model for XMS
    loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                     data = train_XMS,
                                     span = 1, 
                                     degree = 0,
                                     control=loess.control(surface="direct"))
    
    # predict on train and test data
    train_XMS$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_XMS)
    test_XMS$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_XMS)
    
  }, error=function(e){
    
    tryCatch({
      
      # generate a loess model for XMS
      loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                       data = train_XMS,
                                       span = 1, 
                                       degree = 0,
                                       control=loess.control(surface="direct"))
      
      # predict on train and test data
      train_XMS$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_XMS)
      test_XMS$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_XMS)
      
    }, error=function(e){
      
      print(paste0("There may not be enough data for line ", train$line_no[1], " timing point pair ", tpgroup, " to calculate XMS component"))
      
    })
    
  })
  
  # XBX
  train_XBX <- train %>% 
    filter(service_type_code == "XBX")
  test_XBX <- test %>% 
    filter(service_type_code == "XBX")
  
  tryCatch({
    
    # generate a loess model for XBX
    loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                     data = train_XBX,
                                     span = 1, 
                                     degree = 0,
                                     control=loess.control(surface="direct"))
    
    # predict on train and test data
    train_XBX$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_XBX)
    test_XBX$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_XBX)
    
  }, error=function(e){
    
    tryCatch({
      
      # generate a loess model for XBX
      loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                       data = train_XBX,
                                       span = 1, 
                                       degree = 0,
                                       control=loess.control(surface="direct"))
      
      # predict on train and test data
      train_XBX$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_XBX)
      test_XBX$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_XBX)
      
    }, error=function(e){
      
      print(paste0("There may not be enough data for line ", train$line_no[1], " timing point pair ", tpgroup, " to calculate XBX component"))
      
    })
    
  })
  
  # XEM
  train_XEM <- train %>% 
    filter(service_type_code == "XEM")
  test_XEM <- test %>% 
    filter(service_type_code == "XEM")
  
  tryCatch({
    
    # generate a loess model for XEM
    loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                     data = train_XEM,
                                     span = 0.5, 
                                     degree = 0,
                                     control=loess.control(surface="direct"))
    
    # predict on train and test data
    train_XEM$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_XEM)
    test_XEM$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_XEM)
    
  }, error=function(e){
    
    tryCatch({
      
      # generate a loess model for XEM
      loess_year_day_seasonal <- loess(y_trendless_yearseasonless ~ day_sec,
                                       data = train_XEM,
                                       span = 1, 
                                       degree = 0,
                                       control=loess.control(surface="direct"))
      
      # predict on train and test data
      train_XEM$y_year_day_seasonal <- predict(loess_year_day_seasonal, train_XEM)
      test_XEM$yhat_year_day_seasonal <- predict(loess_year_day_seasonal, test_XEM)
      
    }, error=function(e){
      
      print(paste0("There may not be enough data for line ", train$line_no[1], " timing point pair ", tpgroup, " to calculate XEM component"))
      
    })
    
  })
  
  # combine tables for all day types (with daily seasonal trend as a column)
  test <- bind_rows(test_weekday,test_sun, test_sat, test_XEM, test_XBX, test_XMS)
  
  tryCatch({
    
    # make predictions by adding trend, yearly seasonal and daily seasonal components!
    test <- test %>% 
      mutate(predictions = yhat_trend + yhat_year_seasonal + yhat_year_day_seasonal)
    
    # group by UTS and rounding predictions
    test2 <- test %>%
      group_by(sch_min_interval, sch_max_interval, line_no, from_timing_point, to_timing_point, tpp_group, tpp_desc, direction_name, service_type_code, sheet_code) %>%
      summarise_all(funs(mean)) %>%
      mutate(prediction = round(predictions/60)*60) %>%
      select(sch_min_interval, sch_max_interval, line_no, sheet_code, from_timing_point, to_timing_point, tpp_group, tpp_desc, direction_name, service_type_code, prediction, run_time_stamp)
    
    return(test2)
    
  }, error=function(e){
    
    print(paste0("There may not be enough data for line ", train$line_no[1], " timing point pair ", tpgroup, " to perform any calculation"))
    return(NULL)
    
  })

}

# generate test dataframe
predict_df <- generate_predict_sheet(dataframe = data, baseline_sheet_code = "17SEP", predict_sheet_code = "18APR", first_week = 17, last_week = 26)

# let's run the model and predict!
model_df <- predict_schedule(dataframe = data, df_to_predict = predict_df, predict_year = "2018")

sqlQuery_truncate <- "Truncate Table runtime_analysis.final_predictions"
sqlQuery_truncateEXE <- sqlQuery(connection, sqlQuery_truncate)
sqlSave(channel = connection, dat = model_df, tablename = "runtime_analysis.final_predictions", rownames = FALSE, append = TRUE)

#write_csv(model_df, "predictions_new20171220.csv")
