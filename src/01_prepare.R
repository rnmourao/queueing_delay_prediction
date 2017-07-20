# limpa area de trabalho do R
rm(list = ls())

# bibliotecas
library(MASS)

# funcao que calcula a diferenca em segundos de dois horarios
diferenca_em_segundos <- function(hora_depois, hora_antes="00:00:00") {
  antes <- as.numeric(substr(hora_antes, start=1, stop=2)) * 3600 +
    as.numeric(substr(hora_antes, start=4, stop=5)) * 60 +
    as.numeric(substr(hora_antes, start=7, stop=8))
  
  depois <- as.numeric(substr(hora_depois, start=1, stop=2)) * 3600 +
    as.numeric(substr(hora_depois, start=4, stop=5)) * 60 +
    as.numeric(substr(hora_depois, start=7, stop=8))
  
  return (depois - antes)
}

# funcao para normalizar campos numericos
normalizar <- function(x) if (min(x) == max(x)) 0.5 else (x-min(x))/(max(x)-min(x))

## inicio da preparacao de dados
services <- read.csv("/home/mourao/queueing_delay_prediction/data/services.csv", stringsAsFactors = TRUE)

# todos as datas coletadas
these_days <- sort(as.character(unique(services$date)))

## calcular tempos

services$arrival_time <- as.character(services$arrival_time)
services$start_time <- as.character(services$start_time)
services$end_time <- as.character(services$end_time)

# waiting_time
services$waiting_time <- diferenca_em_segundos(services$start_time, services$arrival_time)

# service_time
services$service_time <- diferenca_em_segundos(services$end_time, services$start_time)

# para facilitar os calculos
services$arrival_time_secs <- diferenca_em_segundos(hora_depois = services$arrival_time)
services$start_time_secs <- diferenca_em_segundos(hora_depois = services$start_time)

# remove services com waiting_time menor que zero. Nao ha como obter informacoes relevantes nessa situacao,
# pois o cliente nao foi ao menos chamado.
services <- subset(services, services$waiting_time >= 0)

# substitui por zero service_time negativos.
services$service_time[services$service_time < 0] <- 0

# time_overflow <- alvo
services$time_overflow <- FALSE
services$time_overflow[services$waiting_time >= 1800] <- TRUE

## calcula quantos atendimentos ocorreram no dia anterior

history <- aggregate(x = services$sequence, by = list(date = services$date, branch_id = services$branch_id), FUN=length)
names(history)[3] <- 'count'
history <- history[order(history$branch_id, history$date),]

today <- sort(as.character(unique(history$date)))
tomorrow <- sort(tail(today, -1))
tomorrow <- union(tomorrow, c(NA))
day_n_tomorrow <- data.frame(date = as.Date(today), yesterday = as.Date(today), tomorrow = as.Date(tomorrow))

history <- merge(x = history, y = day_n_tomorrow, all.x = TRUE)
history$date <- NULL
names(history) <- c("branch_id", "yesterday_count", "yesterday", "date")

services <- merge(services, history, all.x = TRUE)

# remove NAs substituindo por media do yesterday_count do branch no mes
for (i in 1:nrow(services)) {
  services$yesterday_count[i] <- trunc(mean(history$yesterday_count[history$branch_id == services$branch_id[i]]))
}

# ordena atendimentos
services <- services[order(services$date, 
                           services$branch_id, 
                           services$queue_id, 
                           services$sequence),]

# monta lista de filas por dia
queues_per_day <- unique(data.frame(date = services$date,
                                    branch_id = services$branch_id,
                                    queue_id = services$queue_id))


# inicializa novas colunas
services$queue_length <- 0
services$clients_count <- 0
services$priorities_count <- 0
services$companies_count <- 0
services$avg_service_time <- 0
services$interarrival_time <- 0
services$arrival_rate <- 0
services$service_rate <- 0
services$utilization_factor <- 0
services$hol <- 0
services$rcs <- 0
services$les <- 0
services$tol <- 0
services$peak_hour <- FALSE

# criacao das novas colunas
for (i in 1:nrow(services)) {
   
   # clients_today: todos os clientes que foram atendidos no mesmo dia e mesma fila que eu.
   services_today <- services[services$date == services$date[i] &
                               services$branch_id == services$branch_id[i] &
                               services$queue_id == services$queue_id[i],]

   # interarrival_time: quanto tempo depois que eu cheguei em relacao ao que chegou imediatamente
   #                    antes de mim
   if (services$sequence[i] != services_today$sequence[1]) {
      services$interarrival_time[i] <- services$arrival_time_secs[i] - 
                                       services$arrival_time_secs[i - 1]
   }
   
   # before: pessoas que chegaram antes de mim e ja foram chamadas
   before <- services[services$date == services$date[i] &
                        services$branch_id == services$branch_id[i] &
                        services$queue_id == services$queue_id[i] &
                        services$sequence < services$sequence[i] &
                        services$start_time_secs < services$arrival_time_secs[i],]

   # served: pessoas que chegaram antes de mim e ja foram atendidas   
   served <- services[before$sequence < services$sequence[i] & before$end_time < services$arrival_time[i],]

   # not_served: pessoas que chegaram antes de mim e ainda nao foram atendidas   
   not_served <- services[before$sequence < services$sequence[i] & before$end_time > services$arrival_time[i],]
      
   # RCS: tempo de espera do ultimo cliente que foi atendido antes de eu chegar.
   if (nrow(served) != 0) {
     services$rcs[i] <- served$waiting_time[nrow(served)]
   }

   # LES: tempo de espera do ultimo cliente que chegou antes de mim e ainda nao terminou
   # de ser atendido.
   if (nrow(not_served) != 0) {
     services$les[i] <- not_served$waiting_time[nrow(not_served)]
   }
      
   # queue: as pessoas que estao na minha frente sao aquelas que chegaram antes de mim, i.e.,
   #        sequence menor que o meu e que ainda nao foram atendidas no momento que eu cheguei
   queue <- services[services$date == services$date[i] &
                       services$branch_id == services$branch_id[i] &
                       services$queue_id == services$queue_id[i] &
                       services$sequence < services$sequence[i] &
                       services$start_time_secs > services$arrival_time_secs[i],]

   # tamanho da fila  
   services$queue_length[i] <- nrow(queue)
   
   # quantidade de correntistas na fila
   services$clients_count[i] <- nrow(queue[!is.na(queue$client_id),])
   
   # quantidade de prioritarios na fila
   services$priorities_count[i] <- nrow(queue[queue$priority == TRUE,])
   
   # quantidade de empresas na fila
   services$companies_count[i] <- nrow(queue[queue$entity == 'company',])
   
   # tempo medio de atendimento da fila (dos que ja foram atendidos)
   if (nrow(before) > 0) {
      services$avg_service_time[i] <- mean(before$service_time)
   }
   
   # HOL: tempo de espera de quem encabeca a fila
   # TOL: tail-of-line - o tempo de espera do cliente que esta na minha frente
   if (nrow(queue) != 0) {
     services$hol[i] <- services$arrival_time_secs[i] - queue$arrival_time_secs[1]
     services$tol[i] <- services$arrival_time_secs[i] - queue$arrival_time_secs[nrow(queue)]
   } 
   
   if (nrow(queue) > 0) {
      interarrival_avg <- mean(queue$interarrival_time)
   } else {
      interarrival_avg <- 0
   }

   # taxa de chegada
   if (interarrival_avg != 0) {
     services$arrival_rate[i] <- 1 / interarrival_avg
   }

   # taxa de servico
   if (services$avg_service_time[i] != 0) {
      services$service_rate[i] <- 1 / services$avg_service_time[i]
   }

   # fator de utilizacao
   if (services$service_rate[i] != 0) {
      services$utilization_factor[i] <- services$arrival_rate[i] / services$service_rate[i]
   }
   
   # identificar se o cliente chegou em um horario de pico de ontem
   if (!is.na(services$yesterday[i])) {
     services_yesterday <- services[services$date == services$yesterday[i] &
                                      services$branch_id == services$branch_id[i] &
                                      services$queue_id == services$queue_id[i],]
     
     # utiliza a funcao hist para obter os intervalos e as frequencias.
     # depois verifica se o horario do atendimento atual esta na faixa de horarios de pico,
     # que defini como a partir do Q3 (terceiro quartil).
     # entao, a quantidade de pessoas que chegaram em algum horario ultrapassou 75% da quantidade
     # do dia anterior, esse horario eh definido como de pico.
     if (nrow(services_yesterday) > 0) {
       h <- hist(services_yesterday$arrival_time_secs, plot = FALSE, breaks = 10)
       q3 <- quantile(h$counts, names = FALSE)[4]
       for (j in 1:length(h$counts)) {
         if (h$counts[j] > q3) {
           if (services$arrival_time_secs[i] >= h$breaks[j] & 
               services$arrival_time_secs[i] <= h$breaks[j + 1]) {
             services$peak_hour[i] <- TRUE
           }
         }        
       }
     }
   }
}

# quantidade de nao correntistas
services$non_clients_count <- services$queue_length - services$clients_count

# quantidade de nao prioritarios
services$non_priorities_count <- services$queue_length - services$priorities_count

# quantidade de pessoas fisicas
services$non_companies_count <- services$queue_length - services$companies_count

# criar campo que unifica branch_id e queue_id. O ultimo se repete entre os branches,
# nao gerando informacao relevante separado do primeiro
services$branch_n_queue <- paste(services$branch_id, "_", services$queue_id, sep = "")

# removendo outliers
services <- services[services$interarrival_time >= 0,]

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

# selecionar somente filas que atendem os pressupostos de teoria de filas.
services <- merge(services, queues[queues$MM1_fit == TRUE,])
services$MM1_fit <- NULL

## separar services_fit em treinamento (70%) e teste (30%)
set.seed(1980)
services$set <- sample.int(n = 2, size = nrow(services), replace = TRUE, prob = c(.7, .3))
services$set[services$set == 1] <- 'train'
services$set[services$set == 2] <- 'test'

## agora tem que preparar o data frame de análise, tirando os campos desnecessarios
services_fit <- services[,c("arrival_rate", "arrival_time_secs", "avg_service_time", 
                            "clients_count", "companies_count", "hol", "interarrival_time", 
                            "les", "non_clients_count", "non_companies_count", 
                            "non_priorities_count", "peak_hour", "priorities_count", 
                            "queue_length", "rcs", "service_rate", "service_type", "tol", 
                            "utilization_factor", "yesterday_count", "time_overflow", "set")]

services_training <- services_fit[services_fit$set == 'train',]
services_test <- services_fit[services_fit$set == 'test',]

# fazer oversampling pois a classe time_overflow = TRUE eh rara (< 20%) e quero privilegia-la.
stf <- services_training[services_training$time_overflow == FALSE,]
stt <- services_training[services_training$time_overflow == TRUE,]
set.seed(1980)
stt <- stt[sample(nrow(stt), nrow(stf), replace=TRUE),]

services_training <- rbind(stf, stt)

# junta tudo de novo
services_fit_oversampled <- rbind(services_training, services_test)

# grava csvs
write.csv(services_fit_oversampled, file = "/home/mourao/queueing_delay_prediction/data/services_fit_over.csv", row.names = FALSE)
# write.csv(services_fit, file = "/home/mourao/queueing_delay_prediction/services_fit.csv", row.names = FALSE)
write.csv(services, file = "/home/mourao/queueing_delay_prediction/data/services_full.csv", row.names = FALSE)
