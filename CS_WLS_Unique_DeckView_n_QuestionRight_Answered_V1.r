##This is File set is to create the Questions Tags and Decks IDs Viewed List 

#Setting uP Working Directory
setwd("C:/Users/theiv/Documents/2019_ISS_MTech_EBAC/Capstone Project/FYP_TeamsStreamz")
#setwd("~/Documents/NUS_EBAC/")


##(WIP)
####1) Matrix for Questions ANswered by Users per language / Country 

#Reading in User Master 

#user_dt = read_csv("Data/user_master.gz")

user_Master = read.csv("Data/user_master.gz",header=TRUE, sep= ",")
user_Master = data.table(user_Master)

View(user_Master)
colnames(user_Master)
# [1] "user_since"     "org_id"         "client_type"    "lang_code"      "role_id"        "country"       
# [7] "location"       "city"           "masked_user_id"

## I remember now that we had taken a decision to not  recommend Streams and Question based on Language but only purley based on Tags. 
## thus country along is enough for the time being. 
# ==> there is lots of Clean up that would be required from the language and country front. 
    
    #lang_country_M = unique(user_Master[,c("lang_code","country")])
    #View(lang_country_M)
    
    #lang_country_user_M = unique(user_Master("masked_user_id","lang_code","country"))
    #userid_M = unique(user_Master[,c("masked_user_id")])
userid_country_M = unique(user_Master[,c("masked_user_id","country")])
    #u_country_M = unique(user_Master[,c("country")])
    #userid_lang_M = unique(user_Master("masked_user_id","lang_code"))
#Removed ==> 
  rm(user_dt)

#Creating the Country Masters for the different data.tables
Ctry_View_M = data.table(unique(views_dt$country))    #22
Ctry_assess_M = data.table(unique(assess_dt$country)) #75
Ctry_master_M = data.table(unique(master_dt$country)) #103


save.image("~/Documents/NUS_EBAC/Data/CS_WLS_Code_withUserData_V2.RData")
#load("~/Documents/NUS_EBAC/Data/Assess_Question_Tag_Searalized_V5.RData") ~ change the file name later on 
load("~/Documents/NUS_EBAC/Data/CS_WLS_Code_withUserData_V2.RData")

## Based on with team, we will need to categorize based on 
# 1) Deck per country , -- available ## CountryDeck_M
# 2) deck per language , -- Not Doing 
# 3) deck per country and language -- Not Doing 
####2)  To Create the Matrix for Streams Viewed by Users

# 1) Question per country , -- available 
# 2) Question per language , -- Not Doing 
# 3) Question per country and language -- Not Doing 
####1)  To Create the Matrix for Questions ANswered by Users 
    colnames(assess_sear_tags_uniq_dt)
# [1] "country"           "question_id"       "org_id"            "role_id"           "submission_utc_ts" "no_of_trials"      "points_earned"     "masked_user_id"    "question_tags"    
# [10] "question_tags.1"   "question_tags.2"   "question_tags.3"   "question_tags.4"  

u_c_q_t_M = unique(assess_sear_tags_uniq_dt[,c("masked_user_id", "country" ,"question_id", "no_of_trials")])  #  "points_earned" 
View(u_c_q_t_M)


u_q_t_M = unique(assess_dt[,c("masked_user_id","question_id", "no_of_trials","points_earned")])  #  "points_earned" 
View(u_q_t_M)

# recommending on a daily basis day : ==> 
# Trail 1 = 10 marks 
# Trail 2 = 5 marks 
# Trail 3 / 4 = 0 marks 

# In DataTable new field creation : 
# DT = data.table(v1=c(1,2,3), v2=2:4)

# DT[, eval(new_var):=v2+5]
# # or
# DT[, (new_var):=v2+5]
# DT

####1)  To Create the Matrix for Questions ANswered by Users D.T = u_q_t_M
new_var <- "qns_ans"
u_q_t_M[,(new_var):=dplyr::case_when(
  qns_ans = points_earned == 10 ~ 1,
  qns_ans = points_earned == 5 ~ 1,
  qns_ans = points_earned == 0 ~ 0)]


####2)  To Create the Matrix for Streams Viewed by Users D.T = u_d_a_M

colnames(views_dt)
#[1] "deck_id","card_id","action","user_action_timestamp","app_version_id" ,"user_since" ,"client_type" , "city"                 
#[9] "country" , "login_handle_type"  , "lang_code" ,  "role_id"  , "masked_user_id"  

u_d_a_M = unique(views_dt[,c("masked_user_id","deck_id","action")])
View(u_d_a_M)

new_var <- "completed"
u_d_a_M[,(new_var):=dplyr::case_when(
  completed = action == "STREAM_COMPLETE" ~ 1,
  completed = action == "STREAM_RECIEVED" ~ 0)]

#AE 
#Has Many langauges =>  we will later on filter the streams and send them out

save.image("Data/V6.RData")
load("Data/V6.RData")
load("C:/Users/theiv/Documents/2019_ISS_MTech_EBAC/Capstone Project/FYP_TeamsStreamz/Data/CS_WLS_Code_withUserData_V2.RData")

