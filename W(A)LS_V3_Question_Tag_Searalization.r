#install.packages("tidyverse")
library(tidyverse)
#install.packages("Hmisc")
library(Hmisc)
library(data.table)
#install.packages("dplyr")
library(dplyr)
#install.packages("R.utils")
library(R.utils)

setwd("C:/Users/theiv/Documents/2019_ISS_MTech_EBAC/Capstone Project/FYP_TeamsStreamz/Data")


assess_dt = read_csv("user_assessments.gz")
#master_dt= read_csv("user_master.gz")
views_dt= read_csv("views_model.gz")

assess_dt = data.table(assess_dt)
#master_dt = data.table(master_dt)
views_dt = data.table(views_dt)


## Creating the Question, COuntry, Tag Master
C_Q_Tag_M = unique(assess_dt %>% select(1,4,9))
    #= View(C_Q_Tag_M %>% group_by(question_id,question_tags,country) %>% summarise(count=n()))

sereliz = C_Q_Tag_M[,2:3]
    #View(sereliz )

#***********************************#
##Searealizing the values of the column = question_tags into 4 columns max
#***********************************#
sereliz_test =  reshape(transform(sereliz, time=ave(question_tags, question_id, FUN=seq_along)), idvar="question_id", direction="wide")
    #View(sereliz_test)
Q_TAQ_SER = sereliz_test[,1:5]
    #View(Q_TAQ_SER)

#Country_Question_Master
C_Q_M = unique(C_Q_Tag_M[,1:2])

#Binding the Searlized Data set of Views wih counties 
C_Q_TAQ_SER= merge(Q_TAQ_SER, C_Q_M,by = c("question_id") ) 
    #,c("country","question_id","question_tags.1","question_tags.2","question_tags.3","question_tags.4")

#Converging the Data Sets across Assess_dt and views_dt for moving it out to the rest of the teams
#1) There are Questiosn ID Duplicates in the C_Q_TAQ_SER.data.table
C_TAQ_SER_4_VIEWDATA = unique(C_Q_TAQ_SER[,2:6])
    #View(C_TAQ_SER_4_VIEWDATA)

#2)#Creating the Country Masters for the different data.tables
Ctry_View_M = data.table(unique(views_dt$country))    #22
    #View(Ctry_View_M)

#3)Lookingup with Views(Streams) ID & Country Master Data_Table 
#Enrich unique stream at country level data with tags from question data - randomise at country level. 


  #Country_Deck Master from Veiws tables
      #stream_data4 = subset(stream_data, select = c(country, deck_id))
      #stream_data4 = stream_data4[duplicated(stream_data4) == FALSE, ]  # Remove dups
  CountryDeck_M = views_dt %>% select(9, 1)
  CountryDeck_M  = unique(CountryDeck_M)
  CountryDeck_M = data.table(CountryDeck_M)



cntry_lst = unique(CountryDeck_M$country)
stream_data5 = data.table()

for ( c in cntry_lst){
  stream_data5 = rbind(stream_data5,cbind(CountryDeck_M[CountryDeck_M$country == c,],C_TAQ_SER_4_VIEWDATA[C_TAQ_SER_4_VIEWDATA$country == c,c("question_tags.1","question_tags.2","question_tags.3","question_tags.4")])
)}
    #head(stream_data5,4)

#4) Merging with the views_dt
#rm(views_sear_tags_dt)

views_sear_tags_dt = merge(views_dt,stream_data5[,c("country","deck_id","question_tags.1","question_tags.2","question_tags.3","question_tags.4")
],by = c("country","deck_id"))

#5) Merging with the assess_dt
assess_sear_tags_dt = merge(assess_dt,C_Q_TAQ_SER[,c("country","question_id","question_tags.1","question_tags.2","question_tags.3","question_tags.4")
                                                 ],by = c("country","question_id"))



#Checks 
filter(stream_data5, stream_data5$deck_id == "stream-de0d5c6a") # Passed 
filter(views_sear_tags_dt, views_sear_tags_dt$deck_id == "stream-de0d5c6a") # Passed 
# country         deck_id question_tags.1 question_tags.2 question_tags.3 question_tags.4
# 1:      US stream-de0d5c6a    tag-13b3f31a    tag-95e2a186    tag-00912fe1    tag-ce0fc331
# 2:      IN stream-de0d5c6a    tag-2a87f6f3    tag-0c80acad    tag-2a87f6f3    tag-0c80acad
# 3:      KE stream-de0d5c6a    tag-70dc242d    tag-d325db69    tag-70dc242d    tag-d325db69
# 4:      GB stream-de0d5c6a    tag-e34b589d    tag-d11834d1    tag-6bc64d6f            <NA>
# 5:      RU stream-de0d5c6a    tag-63f6822b    tag-0f67ac87    tag-63f6822b    tag-0f67ac87
# 6:      IR stream-de0d5c6a    tag-f19f9d99    tag-f19f9d99    tag-f19f9d99    tag-f19f9d99

#merge(views_dt,stream_data5[,c("country","deck_id","question_tags.1","question_tags.2","question_tags.3","question_tags.4")],by = c("country","deck_id"))

# filter(C_Q_Tag_M, C_Q_Tag_M$question_id == "3")
# country question_id question_tags
# 1:      SG           3  tag-13b3f31a
# 2:      SG           3  tag-95e2a186
# 3:      SG           3  tag-90e84621
# 4:      SG           3  tag-ef6a3c07
# 5:      OM           3  tag-13b3f31a
# 6:      OM           3  tag-95e2a186
# 7:      OM           3  tag-90e84621
# 8:      OM           3  tag-ef6a3c07
# 9:      AO           3  tag-13b3f31a
# 10:      AO           3  tag-95e2a186
# 11:      AO           3  tag-90e84621
# 12:      AO           3  tag-ef6a3c07
# 13:      PK           3  tag-13b3f31a
# 14:      PK           3  tag-95e2a186
# 15:      PK           3  tag-90e84621
# 16:      PK           3  tag-ef6a3c07
# 17:      PE           3  tag-13b3f31a
# 18:      PE           3  tag-95e2a186
# 19:      PE           3  tag-90e84621
# 20:      PE           3  tag-ef6a3c07
# 21:      PH           3  tag-13b3f31a
# 22:      PH           3  tag-95e2a186
# 23:      PH           3  tag-90e84621
# 24:      PH           3  tag-ef6a3c07
# 
# filter(C_Q_TAQ_SER, C_Q_TAQ_SER$question_id == "3")
# question_id question_tags.1 question_tags.2 question_tags.3 question_tags.4 country
# 1:           3    tag-13b3f31a    tag-95e2a186    tag-90e84621    tag-ef6a3c07      SG
# 2:           3    tag-13b3f31a    tag-95e2a186    tag-90e84621    tag-ef6a3c07      OM
# 3:           3    tag-13b3f31a    tag-95e2a186    tag-90e84621    tag-ef6a3c07      AO
# 4:           3    tag-13b3f31a    tag-95e2a186    tag-90e84621    tag-ef6a3c07      PK
# 5:           3    tag-13b3f31a    tag-95e2a186    tag-90e84621    tag-ef6a3c07      PE
# 6:           3    tag-13b3f31a    tag-95e2a186    tag-90e84621    tag-ef6a3c07      PH
#
