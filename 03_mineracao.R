# limpa variaveis
# rm(list = ls())

library(randomForest)
library(rfUtilities)

# recuperar bases
services <- read.csv("/home/publico/mestrado/mineracao/raw/gat/services_fit.csv", stringsAsFactors = TRUE)
services$time_overflow <- as.factor(services$time_overflow)

training <- services[services$set == 1,]
validation <- services[services$set == 2,]
test <- services[services$set == 3,]

# treino
fit <- randomForest(x = training[, c(1:20)], 
                    y=training$time_overflow, 
                    do.trace = 100)
plot(fit)
print(fit)

# Call:
#   randomForest(x = training[, c(1:20)], y = training$time_overflow,      do.trace = 100) 
# Type of random forest: classification
# Number of trees: 500
# No. of variables tried at each split: 4
# 
# OOB estimate of  error rate: 10.51%
# Confusion matrix:
#         FALSE TRUE class.error
# FALSE   903  101   0.1005976
# TRUE    110  894   0.1095618


# validacao
# cria diversas combinacoes para os parametros ntree e mtry
ntree <- seq(from = 100, to = 500, by = 100)
mtry <- seq(1:20)
tries <- merge(ntree, mtry)
names(tries) <- c("ntree", "mtry")
tries$accuracy <- 0
tries$fn <- 0
tries$fp <- 0

# itera sobre parametros de randomForest
for (i in 1:nrow(tries)) {
  set.seed(1980)
  temp <- randomForest(x = training[,c(1:20)],
                       y=training$time_overflow,
                       xtest=validation[,c(1:20)],
                       ytest=validation$time_overflow,
                       ntree = tries$ntree[i],
                       mtry = tries$mtry[i])
  
  tries$accuracy[i] <- (temp$test$confusion[1, 1] + temp$test$confusion[2, 2]) / length(temp$test$predicted)
  tries$fn[i] <- temp$test$confusion[2, 1]
  tries$fp[i] <- temp$test$confusion[1, 2]

  cat("\014")
  print(i)
}

# seleciona parametros candidatos que estejam entre as melhores metricas
candidates <- tries[tries$accuracy >= quantile(tries$accuracy)[4] & 
                      tries$fn <= quantile(tries$fn)[2] & tries$fp <= quantile(tries$fp)[2],]


# ntree = 400 mtry = 6 selecionado
set.seed(1980)
validate <- randomForest(x = training[,c(1:20)], 
                                  y=training$time_overflow, 
                                  xtest=validation[,c(1:20)], 
                                  ytest=validation$time_overflow, 
                                  ntree = 400,
                                  mtry = 6)
print(validate)

# mostra validacao
par(mfrow=c(1,1)) 
plot(validate)

# executa teste com dados de training + validation

train_plus_val <- rbind(training, validation)

set.seed(1980)
test.fit <- randomForest(x = train_plus_val[,c(1:20)], 
                         y=train_plus_val$time_overflow, 
                         xtest=test[,c(1:20)], 
                         ytest=test$time_overflow, 
                         ntree = 400,
                         mtry = 6)

print(test.fit)

# acuracia
(2083 + 294) / (2083 + 294 + 52 +83)
