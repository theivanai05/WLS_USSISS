#install.packages("tidyverse")
library(tidyverse)
#install.packages("recosystem")
library(recosystem)

# Saving workspace 
# save.image("C:/Users/Acer/Desktop/Sem4_ACA/CS_WLS_Code_V1_withData.RData")
#load("E:/Theiv_lenovo_Yoga_bckup/theiv/Documents/2019_ISS_MTech_EBAC/2020-SEM4-Advanced_Customer_Analytics/Sem4_ACA/CS_WLS_Code_V1_withData.RData")

load("C:/Users/theiv/Documents/2019_ISS_MTech_EBAC/Capstone Project/FYP_TeamsStreamz/Data/Assess_Question_Tag_Searalized_V5.RData")

setwd("C:/Users/theiv/Documents/2019_ISS_MTech_EBAC/Capstone Project/FYP_TeamsStreamz")

view_df = read_csv("views_model.gz")

class(view_df)
head(view_df)

#install.packages("Hmisc")
library(Hmisc)
statistics = describe(view_df)
str(statistics)

# write uncompressed data
# d %>% write_csv("file.csv")


ps_df = read.csv("pulse_score.csv",header=TRUE, sep= ",")
uq_df = read.csv("UQ_SCore.csv",header=TRUE, sep= ",")
pulsescore_Master = read.csv("Data/master.csv",header=TRUE, sep= ",")

#Making Changes to the Pulse Score File  #129643
pulsescore_Master = pulsescore_Master[,2:4]
pulsescore_Master = pulsescore_Master[complete.cases(pulsescore_Master)==TRUE,]

View(pulsescore_Master)
head(pulsescore_Master)
dim(pulsescore_Master)


# pd_100 = pulsescore_Master[1:100,]
# dim(pd_100) 
# View(pd_100)
# colnames(pd_100)

names <- c('userid' ,'qtag')
pulsescore_Master[,names] <- lapply(pulsescore_Master[,names] , factor)
str(pulsescore_Master)


smp_size = floor(0.8 * nrow(pulsescore_Master))
train_ind = sample(1:nrow(pulsescore_Master),size = smp_size)
train = pulsescore_Master[train_ind,]
test = pulsescore_Master[-train_ind,]


#item_df = as.character(unlist(colnames(ps_df)))
#item_df = as.character(item_df[-1])
#print(item_df)

#class(train_data)
#print

      
train_data = data_memory(train$userid, train$qtag, train$pulsescore,index1 = TRUE)

test_data = data_memory(test$userid, test$qtag, test$pulsescore,index1 = TRUE)

#$train(train_data, out_model = file.path(tempdir(), "model.txt"),opts = list())

recommender = Reco()
recommender$train (train_data,opts=c(dim=10,costp_12 = 0.1,costq_12 = 0.1, 
                                lrate = 0.1, niter = 100, nthread = 6, verbose = F))

show(recommender)
test$prediction = recommender$predict(test_data,out_memory())

test$prediction

# get predictions:  this multiplies the user vectors in testset, with the item vectors in Q
#testevents$prediction <- r$predict(testset, out_memory())   # out_memory means output to memory, can also use "out_file"
#head(testevents)

# compute prediction MAE
test$MAE = abs(test$pulsescore - test$prediction)
mean(test$MAE, na.rm=TRUE) # show the MAE

# we can use the test framework in CFdemolib.r to derive a confusion matrix (assuming any given "like" threshold)
preds = t(test[,c("prediction","pulsescore")])
preds = unlist(preds)
cat("avg MAE =",avgMAE(preds))
#showCM(preds, like=4)
#showCM(preds, like=3)


#Looking at what to recommend to each user : 

test_chk = test %>% filter(test$userid == "002c7b9d") 

tag_recommend = top_n(test_chk,-50,test_chk$prediction)#[,"qtag"]
tag_recommend = tag_recommend["qtag"]

#Given the Tags Recommended; Pull out the questions for the tags
#### Left Join using merge function
df = merge(x=C_Q_TAQ_SER,y=tag_recommend,by.x="question_tags.1",by.y="qtag" )
df = df[c("question_tags.1","question_id","country")] 

# Rename column names
names(df)[names(df) == "question_tags.1"] <- "qtags"


df2 = merge(x=C_Q_TAQ_SER,y=tag_recommend,by.x="question_tags.2",by.y="qtag" )
df2 = df2[c("question_tags.2","question_id","country")]

names(df2)[names(df2) == "question_tags.2"] <- "qtags"

#this is the set of questions recommended for a particular User for a set of 6 Tags... 
#???? ### Yet to Do ### ???? Iterations across all users is yet to be done ???? ### Yet to Do ### ????
qns_recommended = data.table(rbind(df,df2))
qns_recommended = data.table(qns_recommended)
  

## For the TAGS, Need to pick up Questions ANswered Wrongly or Not answered yet 
## Input Required What has the User ANswered and Wrongly Answered  -- DONE 
## for Reduction in Questions that is to be recommended...  -- DONE 

head(qns_recommended)
View(qns_recommended)
#  qtags         question_id  country
# 1 tag-5aed467d       12452      IN
# 2 tag-5aed467d       17135      IN

####1)  Questions ANswered by Users D.T = u_q_t_M

# For user "002c7b9d"
#test_chk = test %>% filter(test$userid == "002c7b9d") 

  # ==> Pass1 : Questions not yet answered 
  u_q_t_M %>% filter( masked_user_id == "002c7b9d" & qns_ans == 0)
  # ==> Pass2 : Questions already answered
  qns_answered_by_user = u_q_t_M %>% filter( masked_user_id == "002c7b9d" & qns_ans == 1)
  qns_answered_by_user = data.table(qns_answered_by_user[,"question_id"])
  #    masked_user_id question_id no_of_trials points_earned qns_ans
  
  # ==> Rate of Questions answered to not answered 
  # ==> Not yet relevant Here


  #Final User unanswered list of questions 
  USER_Qns_Reco = qns_recommended[!qns_answered_by_user, on="question_id"]   
  
  #Users' Country of residence... 
  userid_country_M %>% filter( masked_user_id == "002c7b9d")
  
  #Recommendation Based on the Country for a Single User: 
  USER_Qns_Reco %>% filter(country %in% "IN")
  

#Pull Out the TAG_STREAM Master table
#For the choosen Question Tags (QTAG), pull out the corresponding Stream Tags That has yet to be viewed 
#Output : User Id ==> (DeckID)
  #=> Decks Viewed by user == u_d_a_M
  ####2)  Streams Viewed by Users D.T = u_d_a_M
  View(u_d_a_M)
  
  # ==> Pass1 : STREAMS not yet answered 
  u_d_a_M %>% filter( masked_user_id == "002c7b9d" & completed == 0)
  # ==> Pass2 : STREAMS already answered
  streams_view_by_user = u_d_a_M %>% filter( masked_user_id == "002c7b9d" & completed == 0)
  streams_view_by_user = data.table(streams_view_by_user [,"deck_id"])

  #Final User Unviewed list of STREAMS 
  USER_Stream_Reco = qns_recommended[!streams_view_by_user, on="deck_id"]  
  
  # ==> Streams Data : 
  View(views_sear_tags_uniq_dt)
  View(tag_recommend) 
  
  ## Pulling out Deck IDs for the Corresponding TAGS recommended.   
  sel.col <- c("question_tags.1","question_tags.2","question_tags.3","question_tags.4")
  out.col <- c("masked_user_id","country","deck_id")
  views_sear_tags_uniq_dt[views_sear_tags_uniq_dt[, Reduce(`|`, lapply(.SD, `%in%`, tag_recommend[,c("qtag")])),.SDcols = sel.col],..out.col]
  
  #views_sear_tags_uniq_dt[eval(as.name(sel.col)) == "tag-5aed467d",..view.col]
  #views_sear_tags_uniq_dt[eval(as.name(sel.col)) %in% tag_recommend[,c("qtag")], ..sel.col] -- not ver efficient 
  #views_sear_tags_uniq_dt[get(sel.col[1]) == "tag-5aed467d" | get(sel.col[2])=="tag-5aed467d", ..sel.col]  -- Not using 
  # views_sear_tags_uniq_dt %>% filter(question_tags.4 == "tag-5aed467d" ) --- Test for above data table passed 

  
  #Users' Country of residence... 
  userid_country_M %>% filter( masked_user_id == "002c7b9d")
  
  #Recommendation Based on the Country for a Single User: 
  USER_Qns_Reco %>% filter(country %in% "IN")

#???? ### Yet to Do ### ???? 
#for the corresponding COuntry and Tags, 
#Pick up the Corrsponding Decks IDs
## ?? To DO



# Rest of myMaster RData sets Feeds #
#View(C_Q_M)
#View(C_Q_Tag_M)
#View(C_TAQ_SER_4_VIEWDATA)

View(C_Q_TAQ_SER) # ==> To do this # Has deck_id & question_tags.1 & question_tags.2
colnames(C_Q_TAQ_SER)
# [1] "question_id"     "question_tags.1" "question_tags.2" "question_tags.3" "question_tags.4"
# [6] "country"

View(stream_data5) # ==> To do this # Has deck_id & question_tags.1 & question_tags.2
colnames(stream_data5)
#[1] "country"         "deck_id"         "question_tags.1" "question_tags.2" "question_tags.3"
#[6] "question_tags.4"

#==========================================================================================================#
###Function Libraries : 
  # computes average, mean absolute error
  # each row contains prediction, actual, prediction, actual etc, hence errors are just the diff between consecutive cells
avgMAE = function(preds) {
    plist = unlist(preds)
    errors = sapply(1:(length(plist)/2),function(i) abs(plist[i*2-1]-plist[i*2]))
    errors = errors[errors != Inf]
    mean(errors,na.rm=TRUE)
  }

showCM = function(preds, like) {
  plist = unlist(preds)
  cnts = sapply(1:(length(plist)/2), function(i) {
    pred = plist[i*2-1] ; actual = plist[i*2]
    if (!is.na(pred) & !is.nan(actual)) {
      if (pred>=like) {if(actual>=like) c(1,0,0,0) else c(0,1,0,0)}
      else if(actual<like) c(0,0,1,0) else c(0,0,0,1) 
    } else c(0,0,0,0)
  })
  s = rowSums(cnts)   #returns cnts for: TP, FP, TN, FN
  
  cat(sprintf("TN=%5d FP=%5d\n",s[3],s[2]))
  cat(sprintf("FN=%5d TP=%5d  (total=%d)\n",s[4],s[1], sum(s)))
  cat(sprintf("accuracy  = %0.1f%%\n",(s[1]+s[3])*100/sum(s)))
  cat(sprintf("precision = %3.1f%%\n",s[1]*100/(s[1]+s[2])))
  cat(sprintf("recall    = %3.1f%%\n",s[1]*100/(s[1]+s[4])))
}


