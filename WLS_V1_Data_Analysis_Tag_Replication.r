#install.packages("tidyverse")
library(tidyverse)
#install.packages("Hmisc")
library(Hmisc)
library(data.table)
#install.packages("dplyr")
library(dplyr)

#setwd("C:/Users/Acer/Desktop/Sem4_ACA/FYP_TeamsStreamz/Data")
setwd("C:/Users/theiv/Documents/2019_ISS_MTech_EBAC/Capstone Project/FYP_TeamsStreamz/Data")


assess_df = read_csv("user_assessments.gz")
master_df = read_csv("user_master.gz")
views_df = read_csv("views_model.gz")

assess_dt = data.table(assess_df)
master_dt = data.table(master_df)
views_dt = data.table(views_df)

head(views_dt)
# deck_id       card_id          action user_action_timestamp app_version_id
# 1: stream-de0d5c6a card-bcbe1e2e STREAM_COMPLETE   2019-09-28 19:37:18            751
# 2: stream-430aa58a card-bd0f7479 STREAM_RECIEVED   2019-08-20 05:30:00            751
# 3: stream-e1c1860f card-bd0f7479 STREAM_RECIEVED   2019-08-20 05:30:00            751
# 4: stream-340d951c card-bd0f7479 STREAM_RECIEVED   2019-08-20 05:30:00            751
# 5: stream-3a9441b8 card-bd0f7479 STREAM_RECIEVED   2019-08-20 05:30:00            751
# 6: stream-08a2233a card-bd0f7479 STREAM_RECIEVED   2019-08-20 05:30:00            751
# user_since client_type              city country login_handle_type lang_code
# 1: 2019-04-02 18:58:39     android North Miami Beach      US             Email     EN_US
# 2: 2019-04-02 18:58:39     android North Miami Beach      US             Email     EN_US
# 3: 2019-04-02 18:58:39     android North Miami Beach      US             Email     EN_US
# 4: 2019-04-02 18:58:39     android North Miami Beach      US             Email     EN_US
# 5: 2019-04-02 18:58:39     android North Miami Beach      US             Email     EN_US
# 6: 2019-04-02 18:58:39     android North Miami Beach      US             Email     EN_US
# role_id masked_user_id
# 1:       2       996c47cb
# 2:       2       996c47cb
# 3:       2       996c47cb
# 4:       2       996c47cb
# 5:       2       996c47cb
# 6:       2       996c47cb
head(assess_dt)
# country org_id role_id question_id   submission_utc_ts no_of_trials points_earned masked_user_id
# 1:      US     28       2        9401 2019-08-21 18:24:16            1            10       996c47cb
# 2:      US     28       2        9401 2019-08-21 18:24:16            1            10       996c47cb
# 3:      US     28       2       10034 2019-08-21 18:36:20            1            10       996c47cb
# 4:      US     28       2       10034 2019-08-21 18:36:20            1            10       996c47cb
# 5:      US     28       2       10034 2019-08-21 18:36:20            1            10       996c47cb
# 6:      US     28       2       10035 2019-08-21 18:36:20            3             0       996c47cb
# question_tags
# 1:  tag-a67d9f26
# 2:  tag-d3982a8a
# 3:  tag-a69fe0ec
# 4:  tag-f5503d93
# 5:  tag-4df5ee04
# 6:  tag-a69fe0ec
head(master_dt)
# user_since org_id client_type lang_code role_id country  location              city
# 1: 2015-12-28 00:00:00     NA     android      <NA>      NA    <NA>      <NA>              <NA>
#   2: 2016-01-07 00:00:00     NA     android      <NA>      NA    <NA>      <NA>              <NA>
#   3: 2016-04-01 05:11:14      3     android EN_GLOBAL      NA      IN      <NA>              <NA>
#   4: 2017-05-23 11:17:06     28     android EN_GLOBAL      NA      NG      <NA>              <NA>
#   5: 2017-10-18 14:33:59     28     android     AR_IQ       3      IQ Abbas St.           Karbala
# 6: 2019-04-02 18:58:39     28     android     EN_US       2      US      <NA> North Miami Beach
# masked_user_id
# 1:       ab1fc8a6
# 2:       c983786d
# 3:       7fad26d9
# 4:       b98bc467
# 5:       ece873bb
# 6:       996c47cb

assess_sts = describe(assess_dt)
str(assess_sts)

master_sts = describe(master_dt)
str(master_sts)

views_sts = describe(views_dt)
str(views_sts)


save.image("C:/Users/theiv/Documents/2019_ISS_MTech_EBAC/Capstone Project/FYP_TeamsStreamz/Data/TeamStreamz_Data_V4.RData")
load("C:/Users/theiv/Documents/2019_ISS_MTech_EBAC/Capstone Project/FYP_TeamsStreamz/Data/TeamStreamz_Data_V4.RData")


colnames(views_dt)
# [1] "deck_id"               "card_id"               "action"                "user_action_timestamp"
# [5] "app_version_id"        "user_since"            "client_type"           "city"                 
# [9] "country"               "login_handle_type"     "lang_code"             "role_id"              
# [13] "masked_user_id"  

# Country based Tag pulls.. 
# All CardIds in a Deck Id should have the same Tags
# Check if Tag IDs are replicated across countries 
# Country Level Tag vs Country Level Question randomization ??

#Q1 T1 , T2, T3 
#S1 T3 , T4, 


colnames(master_dt)
# [1] "user_since"     "org_id"         "client_type"    "lang_code"      "role_id"        "country"       
# [7] "location"       "city"           "masked_user_id"

colnames(assess_dt)
# [1] "country"           "org_id"            "role_id"           "question_id"       "submission_utc_ts"
# [6] "no_of_trials"      "points_earned"     "masked_user_id"    "question_tags" 

#Creating the Country Masters for the different data.tables
Ctry_View_M = data.table(unique(views_dt$country))    #22
Ctry_assess_M = data.table(unique(assess_dt$country)) #75
Ctry_master_M = data.table(unique(master_dt$country)) #103

##Master Tag Creations 
#Master_Tags = unique(assess_dt, by="question_tags")
Tags_Master = assess_dt$question_tags
Tags_Master = unique(Tags_Master)
Tags_Master = data.table(Tags_Master)

CountryTag_M = assess_dt %>% select(1, 9)
CountryTag_M  = unique(CountryTag_M )
CountryTag_M  = data.table(CountryTag_M )

CountryUser_M = assess_dt %>% select(1, 9)
CountryUser_M  = unique(CountryUser_M)
CountryUser_M = data.table(CountryUser_M)

CountryDeck_M = views_dt %>% select(9, 1)
CountryDeck_M  = unique(CountryDeck_M)
CountryDeck_M = data.table(CountryDeck_M)


filter(CountryTag_M, question_tags == "tag-e04fd5b5")
# findings ==  stream Ids are nnot specific to Countries 
# So its better to us Deck Master as is. 

TAG_CTRY_CNT = CountryTag_M  %>% group_by(question_tags) %>%  mutate(count = n())
TAG_CTRY_CNT = data.table(TAG_CTRY_CNT)
setnames(TAG_CTRY_CNT, c("countryT","question_tags","count") )
#country, question_tags, count
STRM_CTRY_CNT = CountryDeck_M  %>% group_by(deck_id) %>%  mutate(count = n())
STRM_CTRY_CNT = data.table(STRM_CTRY_CNT)

#TAG_CTRY_CNT %>% plot(TAG_CTRY_CNT$count,unique(TAG_CTRY_CNT$country),xlab="Cnt",ylab="Country Tags are based in",pch=3) 
#TAG_CTRY_CNT %>% plot(TAG_CTRY_CNT$count,TAG_CTRY_CNT$country,xlab="Cnt",ylab="Country Tags are based in",pch=3)

# finding out how many countries this TAG is related to out of a total of 75 countries in assessment master
filter(TAG_CTRY_CNT,question_tags == "tag-a67d9f26") # cnt = 32


filter(CountryDeck_M, deck_id == "stream-a0a27012")
# findings ==  stream Ids are nnot specific to Countries 
# So its better to us Deck Master as is. 

Deck_Master = views_dt$deck_id
Deck_Master = unique(Deck_Master)
Deck_Master = data.table(Deck_Master)


Question_Master = assess_dt$question_id
Question_Master = unique(Question_Master)
Question_Master = data.table(Question_Master)

QnTag_Master =  assess_dt %>% select(4, 9)
QnTag_Master =  unique(QnTag_Master)
class(QnTag_Master)

