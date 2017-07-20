# limpa variaveis
rm(list = ls())

library(pROC)

confusion_matrix <- function(observed, predicted) {
  if (length(observed) != length(predicted)) 
    stop("Observed and predicted vectors must have same length.")
  
  tp = 0
  tn = 0
  fp = 0
  fn = 0
  for (i in 1:length(observed)) {
    if (observed[i] == TRUE & predicted[i] == TRUE) tp = tp + 1
    if (observed[i] == FALSE & predicted[i] == FALSE) tn = tn + 1
    if (observed[i] == FALSE & predicted[i] == TRUE) fp = fp + 1
    if (observed[i] == TRUE & predicted[i] == FALSE) fn = fn + 1
  }
  
  m <- data.frame('T' = c(0, 0), 'F' = c(0, 0))
  rownames(m) <- c("T", "F")
  
  m[1, 1] <- tp
  m[1, 2] <- fn
  m[2, 1] <- fp
  m[2, 2] <- tn
  
  return(m)
}

f1 <- function(TP, FP, FN) {
  p <- TP / (TP + FP)
  r <- TP / (TP + FN)
  return(2 / ((1/p) + (1/r)))
}

Wq <- function(rho, lambda, mu, secs) rho * exp( -secs * (mu - lambda) )

# recuperar atendimentos
services <- read.csv("/home/mourao/queueing_delay_prediction/data/services_fit_over.csv", stringsAsFactors = TRUE)
services$TO_bin <- as.numeric(services$time_overflow)

train <- services[services$set == 'train',]
test <- services[services$set == 'test',]

# prever usando equacao da teoria de filas
train$predicted_perc <- Wq(rho = train$utilization_factor, 
                           lambda = train$arrival_rate,
                           mu = train$service_rate,
                           secs = 1800) # 30 minutos

# ROC e AUC
train_roc <- roc(TO_bin ~ predicted_perc, data=train)
plot(train_roc, legacy.axes = TRUE, identity.col="red", col="darkblue", 
     grid=TRUE, grid.lty=1, xlim=c(1, 0), ylim=c(0, 1), las=1)

# validar formula
perc <- seq(from = 0, to = 1, by = .01)
tries <- data.frame(perc = perc, f1 = 0, tp = 0, tn = 0, fn = 0, fp = 0)

for (i in 1:nrow(tries)) {
  train$predicted <- FALSE
  train$predicted[train$predicted_perc > tries$perc[i]] <- TRUE
  
  # criar matriz de confusao
  tries$tp[i] <- nrow(train[train$time_overflow == TRUE & train$predicted == TRUE,])
  tries$tn[i] <- nrow(train[train$time_overflow == FALSE & train$predicted == FALSE,])
  tries$fp[i] <- nrow(train[train$time_overflow == FALSE & train$predicted == TRUE,])
  tries$fn[i] <- nrow(train[train$time_overflow == TRUE & train$predicted == FALSE,])
    
  # medir f1
  tries$f1[i] <- f1(tries$tp[i], tries$fp[i], tries$fn[i])
}

# escolhe modelo com melhor f1-measure
perc <- min(tries$perc[tries$f1 == max(tries$f1[tries$perc >= .5])])
# com oversampling: 50%

train$predicted <- FALSE
train$predicted[train$predicted_perc > perc] <- TRUE

print(confusion_matrix(train$time_overflow, train$predicted))

#      T    F
# T 1110 3614
# F  833 3891

## executa teste
test$predicted_perc <- Wq(rho = test$utilization_factor, 
                          lambda = test$arrival_rate,
                          mu = test$service_rate,
                          secs = 1800)

# ROC e AUC
test_roc <- roc(TO_bin ~ predicted_perc, data=test)
plot(test_roc, legacy.axes = TRUE, identity.col="red", col="darkblue", 
     grid=TRUE, grid.lty=1, xlim=c(1, 0), ylim=c(0, 1), las=2)
auc(test_roc)

test$predicted <- FALSE
test$predicted[test$predicted_perc > perc] <- TRUE

# criar matriz de confusao
tp <- nrow(test[test$time_overflow == TRUE & test$predicted == TRUE,])
tn <- nrow(test[test$time_overflow == FALSE & test$predicted == FALSE,])
fp <- nrow(test[test$time_overflow == FALSE & test$predicted == TRUE,])
fn <- nrow(test[test$time_overflow == TRUE & test$predicted == FALSE,])

# medir f1-measure
print(paste("f1-measure", f1(TP=tp, FP=fp, FN=fn)))
# "f1-measure 0.122448979591837"

# mostrar matriz de confusao do teste
print(confusion_matrix(test$time_overflow, test$predicted))

#     T    F
# T  33  102
# F 371 1657

# # criar cross-validation e grid
# set.seed(1980)
# train <- train[sample(nrow(train)),]
# folds <- cut(seq(1, nrow(train)), breaks=5, labels=FALSE)
# 
# ## cv
# for(i in 1:5){
#   v <- which(folds==i, arr.ind=TRUE)
#   cv_val <- train[v, ]
#   cv_train <- train[-v, ]
#   ...
# }