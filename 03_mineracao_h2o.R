# clear environment
# h2o.shutdown(prompt = FALSE)
rm(list = ls())

# Load the H2O library and start up the H2O cluter locally on your machine
library(h2o)
h2o.init(nthreads = 6,         #Number of threads -1 means use all cores on your machine
         max_mem_size = "8G")  #max mem size is the maximum memory to allocate to H2O

# Import the data
services_csv <- '/home/mourao/queueing_delay_prediction/services_fit.csv' 
data <- h2o.importFile(services_csv)  
dim(data) # 7,075 rows x 22 columns

# Removed previous split column. The data will be split by h2o.
data$set <- NULL

# Partition the data into training and test sets
splits <- h2o.splitFrame(data = data, 
                         ratios = 0.7,
                         seed = 1)
train <- splits[[1]]
test <- splits[[2]]

# Take a look at the size of each partition
# Notice that h2o.splitFrame uses approximate splitting not exact splitting (for efficiency)
# so these are not exactly 70% and 30% of the total rows
nrow(train)  # 4968
nrow(test)   # 2107


# Identify response and predictor variables
y <- "time_overflow"
x <- setdiff(names(data), y)  
print(x)
# [1] "service_type"         "arrival_time_secs"    "yesterday_count"      "queue_length"        
# [5] "clients_count"        "priorities_count"     "companies_count"      "avg_service_time"    
# [9] "interarrival_time"    "arrival_rate"         "service_rate"         "utilization_factor"  
# [13] "hol"                  "rcs"                  "les"                  "tol"                 
# [17] "peak_hour"            "non_clients_count"    "non_priorities_count" "non_companies_count" 

# Now that we have prepared the data, we can train some models
# Rather than training models manually one-by-one, we will make
# use of the h2o.grid function to train a bunch of models at once

# Random Grid Search
# This is set to run fairly quickly, increase max_runtime_secs 
# or max_models to cover more of the hyperparameter space.
# Also, you can expand the hyperparameter space of each of the 
# algorithms by modifying the hyper param code below.


# Next we will explore some of the deep learning
# hyperparameters in a random grid search

# Deeplearning hyperparamters
activation_opt <- c("Rectifier", "RectifierWithDropout", "Maxout", "MaxoutWithDropout")
l1_opt <- c(0, 0.00001, 0.0001, 0.001, 0.01, 0.1)
l2_opt <- c(0, 0.00001, 0.0001, 0.001, 0.01, 0.1)
hyper_params <- list(activation = activation_opt,
                     l1 = l1_opt,
                     l2 = l2_opt)
search_criteria <- list(strategy = "RandomDiscrete", 
                        max_models = 50)


dl_grid <- h2o.grid("deeplearning", x = x, y = y,
                    grid_id = "dl_grid",
                    training_frame = train,
                    nfolds = 5,
                    seed = 1,
                    hidden = c(10,10),
                    hyper_params = hyper_params,
                    search_criteria = search_criteria)


## Gradient Boosting Machine

gbm_params <- list(ntrees = seq(100, 500, 50),
                   learn_rate = seq(0.1, 0.3, 0.01), 
                   max_depth = seq(2, 10, 1),
                   sample_rate = seq(0.5, 1.0, 0.05),  
                   col_sample_rate = seq(0.1, 1.0, 0.1))
search_criteria <- list(strategy = "RandomDiscrete", 
                        max_models = 50)  

gbm_grid <- h2o.grid("gbm", x = x, y = y,
                     grid_id = "gbm_grid",
                     training_frame = train,
                     nfolds=5,
                     seed = 1,
                     hyper_params = gbm_params,
                     search_criteria = search_criteria)

### Random Forest

rf_params <- list(ntrees = seq(100, 500, 50), 
                  mtries = seq(1, 20, 1))
search_criteria <- list(strategy = "RandomDiscrete", 
                        max_models = 50)  

rf_grid <- h2o.grid("randomForest", x = x, y = y,
                     grid_id = "rf_grid",
                     training_frame = train,
                     nfolds=5,
                     seed = 1,
                     hyper_params = rf_params,
                     search_criteria = search_criteria)

###### Test #####


### DL

dl_gridperf <- h2o.getGrid(grid_id = "dl_grid", 
                           sort_by = "f1", 
                           decreasing = TRUE)
print(dl_gridperf)

# Note that that these results are not reproducible since we are not using a single core H2O cluster
# H2O's DL requires a single core to be used in order to get reproducible results

# Grab the model_id for the top DL model, chosen by validation F-Measure
best_dl_model_id <- dl_gridperf@model_ids[[1]]
best_dl <- h2o.getModel(best_dl_model_id)

# Now let's evaluate the model performance on a test set
# so we get an honest estimate of top model performance
best_dl_perf <- h2o.performance(model = best_dl,
                                xval = TRUE,
                                newdata = test)
h2o.F1(best_dl_perf)
h2o.confusionMatrix(best_dl, test)

### GBM

gbm_gridperf <- h2o.getGrid(grid_id = "gbm_grid", 
                            sort_by = "f1", 
                            decreasing = TRUE)
print(gbm_gridperf)

# Grab the model_id for the top GBM model, chosen by validation F-Measure
best_gbm_model_id <- gbm_gridperf@model_ids[[1]]
best_gbm <- h2o.getModel(best_gbm_model_id)

# # Now let's evaluate the model performance on a test set
# # so we get an honest estimate of top model performance
best_gbm_perf <- h2o.performance(model = best_gbm, 
                                 xval = TRUE,
                                 newdata = test)
h2o.F1(best_gbm_perf)
h2o.confusionMatrix(best_gbm, test)

### RF

rf_gridperf <- h2o.getGrid(grid_id = "rf_grid", 
                           sort_by = "f1", 
                           decreasing = TRUE)
print(rf_gridperf)

# Grab the model_id for the top RF model, chosen by validation F-Measure
best_rf_model_id <- rf_gridperf@model_ids[[1]]
best_rf <- h2o.getModel(best_rf_model_id)

# # Now let's evaluate the model performance on a test set
# # so we get an honest estimate of top model performance
best_rf_perf <- h2o.performance(model = best_rf, 
                                xval = TRUE,
                                newdata = test)
h2o.F1(best_rf_perf)
h2o.confusionMatrix(best_rf, test)
