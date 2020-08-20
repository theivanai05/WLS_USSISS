## Creating Unique Country List and Later Questions Tags for the 75 COuntries in the Assess_Views

#Required Tables: 
  # CountryTag_M
  # Ctry_assess_M

#US_TAG_M: 
#table_Name = paste(trimws(toString(Ctry_assess_M$V1[1])),"_TAG_M")
#table_Name = paste(Ctry_assess_M$V1[1],"_TAG_M")
US_TAG_M = data.frame(CountryTag_M %>% filter(country == "US"))
IN_TAG_M = CountryTag_M %>% filter(country == "IN")

us_assess_df = assess_df %>% filter(country == "US")


cl <- makeCluster(3, type="SOCK") # for 3 cores machine
registerDoSNOW (cl)
#condition <- (df$col1 + df$col2 + df$col3 + df$col4) > 4
# parallelization with vectorization
system.time({#nrow(df)
  output <- foreach(i = 1:nrow((assess_df)), .combine=rbind) %dopar% 
  {
    #kimisc::sample.rows(dplyr::filter(TAG_CTRY_CNT,TAG_CTRY_CNT$countryT == toString(assess_df[i,"country"]))[,2],runif(1, 1, 4))
    kimisc::sample.rows(US_TAG_M[,2],runif(1, 1, 4))
  }
})
output

df$output <- output
stopCluster(cl)


### Reading in Dinakar's Files to check on the Tags...
setwd("C:/Users/theiv/Documents/2019_ISS_MTech_EBAC/Capstone Project/FYP_TeamsStreamz/Data/Tagged_W_Dinakar")


assess_d_wt_df = read_csv("user_assessments_withtags.csv")
views_d_wt_df = read_csv("views_model_withtags.csv")



  
  