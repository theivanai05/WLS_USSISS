# parallel processing
#install.packages("foreach")
#install.packages("doSNOW")
install.packages("svMisc") #To make a progress output
library(foreach)
library(doSNOW)
library(kimisc)
library(tidyverse)
library(Hmisc)
library(data.table)
library(dplyr)
library(svMisc)


cl <- makeCluster(3, type="SOCK") # for 3 cores machine
registerDoSNOW (cl)
system.time({
  output <- foreach(i = 1:100,.combine=rbind) %dopar% 
  {
    #kimisc::sample.rows(dplyr::filter(TAG_CTRY_CNT,TAG_CTRY_CNT$countryT == toString(assess_df[i,"country"]))[,2],runif(1, 1, 4))
    ctry = toString(assess_df[i,"country"])
    Ctry_list = data.frame(dplyr::filter(CountryTag_M,CountryTag_M$country == ctry ))
    kimisc::sample.rows(data.frame(Ctry_list),runif(1, 1, 4),rep=T)
    
    #svMisc::progress(i,progress.bar = TRUE)
    #Sys.sleep(0.01)
    #if (i == nrow(assess_df)) cat("Done!\n")
    
  }
})
output

assess_df$question_tags2 <- output
stopCluster(cl)

