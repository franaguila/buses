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
tp.sch_run_time, tp.from_act_leave_datetime, hrp.mean_act_rt, getdate() as run_time_stamp
FROM runtime_analysis.tp_data tp

LEFT JOIN runtime_analysis.hrp hrp ON
tp.from_timing_point=hrp.from_timing_point AND tp.to_timing_point=hrp.to_timing_point 
AND tp.sch_min_interval=hrp.sch_min_interval AND tp.sch_max_interval=hrp.sch_max_interval 
AND tp.line_no=hrp.line_no AND tp.direction_name=hrp.direction_name
AND tp.service_type_code=hrp.service_type_code AND tp.sheet_code=hrp.sheet_code"

# connect to Savona db
data <- sqlQuery(connection, sqlQuery_view)

# this is a function that takes in a data frame with predictions
# calculates the root mean squared error of CMBC's predictions and the model's predictions
# and returns a data frame with line number and the respective errors for each line number
# this is what is run last...
score_predictions <- function(dataframe){
  
  scores_df <- dataframe %>% 
    drop_na(prediction) %>%   # gets rid of all the NA values that skew our comparison
    group_by(line_no) %>%   # grouping by line
    summarise(cmbc_error = sqrt(mean((sch_run_time - mean_act_rt)^2, na.rm = TRUE)),   # calculates root mean squared error for CMBC's predictions at the Line Level
              ts_model_error = sqrt(mean((prediction - mean_act_rt)^2, na.rm = TRUE)))   # calculates root mean squared error for model predictions
  
  return(scores_df)
  
}

# this is a function that takes in a data frame, an end date for the training data, and a start date for the test/validation data
# splits the data by line number, uses map to speed up predictions, combines timing point group data frames
# and returns a final data frame similar to the input, with an added column of model predictions
predict_schedule_for_validation <- function(dataframe, train_end, test_start){
  
  # use the next two lines of code if you want to test certain bus lines
  # dataframe <- dataframe %>%
  #   filter(line_no == "480")
  
  # creating a list of lines for the for loop
  line_list <- levels(factor(dataframe$line_no))
  # initializing an empty data frame
  predictions_dfs <- data.frame(sch_min_interval = numeric(0), sch_max_interval = numeric(0), sch_run_time = numeric(0), 
                                prediction = numeric(0), mean_act_rt = numeric(0))
  
  # loop over all the lines
  for(line in line_list){
    
    print(paste0("line:", line))
    df1 <- dataframe %>% 
      filter(line_no == line) %>% 
      mutate(year = year(from_act_leave_datetime),
             week = week(from_act_leave_datetime),
             weekyear = year + (2*week-1)/(52*2),
             day_sec = from_act_leave_time)
    
    # split into train and test data
    train1 <- df1 %>% 
      filter(operating_date <= train_end)
    test1 <- df1 %>% 
      filter(operating_date >= test_start)
    
    # make sure we have data for both train and test
    if(nrow(train1) == 0 | nrow(test1) == 0){   # conditional to make sure we have data for both train and test
    } else{
      
      # create list of timing point group pairs
      tpp_list <- levels(factor(train1$tpp_group))  # create list of timing point group pairs
      
      # call the next function that contains the model
      predictions_df <- map(tpp_list, processing_scoring, train = train1, test = test1)
      
      # combine all the timing point group pair data for this specific line
      if(is_empty(unlist(predictions_df))){
        
      } else {
        
        predictions_df <- bind_rows(bind_rows(predictions_df))
        
        # combine all the bus lines
        predictions_dfs <- full_join(predictions_df, predictions_dfs)
        
      }
    }
  }
  
  return(predictions_dfs)
}

# this function takes in a timing point group, training data, and test/validation data
# trains the trend decomposition model on the training data and generates predictions on test data
# this function returns a dataframe with predictions for a specific timing point group
processing_scoring <- function(tpgroup, train, test){
  
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
      group_by(sch_min_interval, sch_max_interval, line_no, from_timing_point, to_timing_point, tpp_desc, direction_name, service_type_code, sheet_code) %>%
      summarise_all(funs(mean)) %>%
      mutate(prediction = round(predictions/60)*60) %>%
      select(sch_min_interval, sch_max_interval, line_no, sheet_code, from_timing_point, to_timing_point, tpp_desc, direction_name, service_type_code, prediction, sch_run_time, mean_act_rt, run_time_stamp)

    return(test2)
    
  }, error=function(e){
    
    print(paste0("There may not be enough data for line ", train$line_no[1], " timing point pair ", tpgroup, " to perform any calculation"))
    
    # return nothing
    return()
    
  })
  
}

# let's run the model!
model_df_for_validation <- predict_schedule_for_validation(dataframe = data, train_end = "2017-09-03", test_start = "2017-09-04")

# now let's check the errors for each line
Validation_errors <- score_predictions(dataframe = model_df_for_validation)
