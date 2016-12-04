# limpa variaveis
# rm(list = ls())

library(MASS)

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

Wq <- function(rho, lambda, mu, secs) rho * exp( -secs * (mu - lambda) )


# recuperar atendimentos
services <- read.csv("/home/publico/mestrado/mineracao/raw/gat/services_full.csv", stringsAsFactors = TRUE)

# executar testes de aderencia a fim de verificar se filas possuem pressupostos da teoria de filas.
# os pressupostos sao que as metricas interarrival_time e service_time sigam a distribuicao
# exponencial.
queues <- unique(data.frame(branch_n_queue = services$branch_n_queue, date = services$date))
queues$MM1_fit <- FALSE

for (i in 1:nrow(queues)) {
  arrival_fit <- FALSE
  service_fit <- FALSE
  
  temp <- services[services$branch_n_queue == queues$branch_n_queue[i] &
                     services$date == queues$date[i],]
  
  ## testando interarrival_rate
  fit <- fitdistr(temp$interarrival_time, "exponential")
  test <-  ks.test(temp$interarrival_time, "pexp", fit$estimate)  
  if (test$p.value > 0.05) {
    arrival_fit <- TRUE
  }
  ## testando service_time
  fit <- fitdistr(temp$service_time, "exponential")
  test <-  ks.test(temp$service_time, "pexp", fit$estimate)  
  if (test$p.value > 0.05) {
    service_fit <- TRUE
  }
  
  if (arrival_fit == TRUE & service_fit == TRUE) {
    queues$MM1_fit[i] <- TRUE
  }
}

# selecionar somente filas que atendem os pressupostos de teoria de filas. (190 fora)
services <- merge(services, queues[queues$MM1_fit == TRUE,])



validation <- services[services$set == 2,]
test <- services[services$set == 3,]

# prever usando equacao da teoria de filas
validation$predicted_perc <- Wq(rho = validation$utilization_factor, 
                                lambda = validation$arrival_rate,
                                mu = validation$service_rate,
                                secs = 1800)

# validar formula
perc <- seq(from = .5, to = .95, by = .05)
tries <- data.frame(perc = perc, accuracy = 0, tp = 0, tn = 0, fn = 0, fp = 0)

for (i in 1:nrow(tries)) {
  validation$predicted <- FALSE
  validation$predicted[validation$predicted_perc > tries$perc[i]] <- TRUE
  
  # criar matriz de confusao
  tries$tp[i] <- nrow(validation[validation$time_overflow == TRUE & validation$predicted == TRUE,])
  tries$tn[i] <- nrow(validation[validation$time_overflow == FALSE & validation$predicted == FALSE,])
  tries$fp[i] <- nrow(validation[validation$time_overflow == FALSE & validation$predicted == TRUE,])
  tries$fn[i] <- nrow(validation[validation$time_overflow == TRUE & validation$predicted == FALSE,])
    
  # medir acuracia
  tries$accuracy[i] <- (tries$tp[i] + tries$tn[i]) / nrow(validation)
}

# escolhe modelo com melhor acuracia
tries[tries$accuracy == max(tries$accuracy),]

#   perc  accuracy tp   tn fn  fp
#10 0.95 0.7959184 17 1114 55 235

validation$predicted <- FALSE
validation$predicted[validation$predicted_perc > .9] <- TRUE

print(confusion_matrix(validation$time_overflow, validation$predicted))

#     T    F
# T  17   55
# F 236 1113

## executa teste
test$predicted_perc <- Wq(rho = test$utilization_factor, 
                          lambda = test$arrival_rate,
                          mu = test$service_rate,
                          secs = 1800)

test$predicted <- FALSE
test$predicted[test$predicted_perc > .95] <- TRUE

# criar matriz de confusao
tp <- nrow(test[test$time_overflow == TRUE & test$predicted == TRUE,])
tn <- nrow(test[test$time_overflow == FALSE & test$predicted == FALSE,])
fp <- nrow(test[test$time_overflow == FALSE & test$predicted == TRUE,])
fn <- nrow(test[test$time_overflow == TRUE & test$predicted == FALSE,])

# medir acuracia
print(paste("accuracy", (tp + tn) / nrow(test)))

# "accuracy 0.784637473079684"

# mostrar matriz de confusao do teste
print(confusion_matrix(test$time_overflow, test$predicted))

#     T    F
# T  20   77
# F 223 1073


# plot a graph
# hist(ex, freq = FALSE, breaks = 100, xlim = c(0, quantile(ex, 0.99)))
# curve(dexp(x, rate = fit1$estimate), col = "red", add = TRUE)