## https://datascienceplus.com/strategies-to-speedup-r-code/
## Source Code for 
##https://nceas.github.io/oss-lessons/parallel-computing-in-r/parallel-computing-in-r.html

install.packages("parallel")
install.packages("MASS")
library(parallel)
library(MASS)

# i have 8 Cores 
#detectCores()

# Create the data frame
col1 <- runif (12^5, 0, 2)
col2 <- rnorm (12^5, 0, 2)
col3 <- rpois (12^5, 3)
col4 <- rchisq (12^5, 2)
df <- data.frame (col1, col2, col3, col4)

# Original R code: Before vectorization and pre-allocation
system.time({
  for (i in 1:nrow(df)) { # for every row
    if ((df[i, "col1"] + df[i, "col2"] + df[i, "col3"] + df[i, "col4"]) > 4) { # check if > 4
      df[i, 5] <- "greater_than_4" # assign 5th column
    } else {
      df[i, 5] <- "lesser_than_4" # assign 5th column
    }
  }
})
#user  system elapsed 
#141.27  146.21  288.96 

# Thanks to Gabe Becker
system.time({
  want = which(rowSums(df) > 4)
  output = rep("less than 4", times = nrow(df))
  output[want] = "greater than 4"
}) 

// Source for MyFunc.cpp
#include 
using namespace Rcpp;
// [[Rcpp::export]]
CharacterVector myFunc(DataFrame x) {
  NumericVector col1 = as(x["col1"]);
  NumericVector col2 = as(x["col2"]);
  NumericVector col3 = as(x["col3"]);
  NumericVector col4 = as(x["col4"]);
  int n = col1.size();
  CharacterVector out(n);
  for (int i=0; i 4){
    out[i] = "greater_than_4";
  } else {
    out[i] = "lesser_than_4";
  }
}
return out;
}

library(Rcpp)
sourceCpp("MyFunc.cpp")
system.time (output <- myFunc(df)) # see Rcpp function below

dt <- data.table(df)  # create the data.table
system.time({
  for (i in 1:nrow (dt)) {
    if ((dt[i, col1] + dt[i, col2] + dt[i, col3] + dt[i, col4]) > 4) {
      dt[i, col5:="greater_than_4"]  # assign the output as 5th column
    } else {
      dt[i, col5:="lesser_than_4"]  # assign the output as 5th column
    }
  }
})



# parallel processing
#install.packages("foreach")
#install.packages("doSNOW")
library(foreach)
library(doSNOW)
library(kimisc)
library(tidyverse)
library(Hmisc)
library(data.table)
library(dplyr)

cl <- makeCluster(3, type="SOCK") # for 3 cores machine
registerDoSNOW (cl)
system.time({
  output <- foreach(i = 1:nrow(us_assess_df),.combine=rbind) %dopar% 
    {
      #kimisc::sample.rows(dplyr::filter(TAG_CTRY_CNT,TAG_CTRY_CNT$countryT == toString(assess_df[i,"country"]))[,2],runif(1, 1, 4))
      Ctry_list = US_TAG_M[,2]
      kimisc::sample.rows(data.frame(Ctry_list),runif(1, 1, 4),rep=T)
    }
})
output

us_assess_df$question_tags3 <- output
stopCluster(cl)

head(us_assess_df)


#filter(TAG_CTRY_CNT,TAG_CTRY_CNT$countryT == "US")[,2]
#filter(TAG_CTRY_CNT,TAG_CTRY_CNT$countryT == toString(assess_dt[1,"country"]))[,2]
#toString(assess_dt[1,"country"])

#With 3 Cores 
#user  system elapsed 
#86.71   52.94  144.40

# With 6 Cores 
#user  system elapsed 
#448.37  130.65  587.45



for (i in 1:cyc) {
  question_tags_IP <- sample.rows(filter(TAG_CTRY_CNT,country == toString(assess_dt[i,country]))[,2],runif(1, 1, 4),rep=T)
  question_tags2 <- append(question_tags2,list(question_tags_IP))

}

#df[i, "col4"]
#dt[i, col3]
#assess_dt$country[a]
#assess_dt[i,country]

assess_tag_dt <- data.table(question_tags2)
View(assess_tag_dt)




# parallel processing
#install.packages("foreach")
#install.packages("doSNOW")
#install.packages("lme4")
library(foreach)
library(doSNOW)
library(lme4)


#cl <- makeCluster(5, type="SOCK") # for 6 cores machine
#registerDoSNOW (cl)
# parallelization

cyc= nrow(assess_dt)
nworkers <- detectCores()

f <- function(i) {
  sample.rows(filter(TAG_CTRY_CNT,country == toString(assess_dt[i,"country"]))[,2],runif(1, 1, 4),rep=T)
}
#nrow(assess_dt)
system.time(save <- mclapply(1:cyc, f, mc.cores=nworkers))

#stopCluster(cl)


####Trying Lapply 
f <- function(i)
{
  Ctry_list = US_TAG_M[,2]
  question_tag2[i] = sample.rows(Ctry_list,runif(1, 1, 4),rep=T)
  question_tag3[i] = sample.rows(Ctry_list,runif(1, 1, 4),rep=T)
}

lapply(df,f[i])





###Trying out the parLapply without fork asfork is for unix
cl <- makeCluster(nworkers)
clusterSetRNGStream(cl, nrow(df)) #make the bootstrapping exactly the same as above to equate computation time
clusterExport(cl, c("US_TAG_M","df")) #note that by default, objects are not shared with workers -- we need to 'export' them manually
res_parLapply <- parLapply(cl, 1:nrow(df), function(i) {
  #ind <- sample(nobs, nobs, replace=TRUE) #indices for resampling with replacement
  #shuf_df <- iris_2species[ind,]
  
  #m_out <- suppressWarnings(glm(Species ~ Sepal.Length, shuf_df, family="binomial"))
  #broom::tidy(m_out) #handy function for building data.frame of model coefficients
  Ctry_list = US_TAG_M[,2]
  kimisc::sample.rows(data.frame(Ctry_list),runif(1, 1, 4),rep=T)
})

res_parLapply <- dplyr::bind_rows(res_parLapply)
stopCluster(cl)

res_parLapply



