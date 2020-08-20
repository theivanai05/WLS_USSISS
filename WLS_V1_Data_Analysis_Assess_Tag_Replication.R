#install.packages("missMethods")
library(missMethods)
install.packages('kimisc')
library(kimisc)


set.seed(1200)
cyc= as.integer(nrow(assess_dt))
N_Blocks = 1
a=1

question_tags_IP <- data.table()
question_tags_IP3 <- data.table()
question_tags2 <- data.table()
question_tags3 <- data.table()
assess_tag_dt  <- data.table()

#assess_dt %>% mutate(question_tags = sample( ,1,rep=T) )

#class(toString(assess_dt$country[1]))
#toString(assess_dt$country[1])

#Step-by-step to lookup values and pick-up 1 - 3 sample records
filter(TAG_CTRY_CNT,country == "US")[,2]
filter(TAG_CTRY_CNT,country == toString(assess_dt[1,country]))[,2]
### FINALLY WORKED ###
sample.rows(filter(TAG_CTRY_CNT,country == toString(assess_dt[1,country]))[,2],runif(1, 1, 4),rep=T)
sample.rows(filter(TAG_CTRY_CNT,country == toString(assess_dt[1,country]))[,2],runif(1, 1, 4))
sample.rows(filter(TAG_CTRY_CNT,country == assess_dt[1,country])[,2],runif(1, 1, 4))


print(question_tags_IP)
print(question_tags2)


for (a in 1:cyc) {
  question_tags_IP <- sample.rows(filter(TAG_CTRY_CNT,country == toString(assess_dt$country[a]))[,2],runif(1, 1, 4),rep=T)
  question_tags2 <- append(question_tags2,list(question_tags_IP))
  #a = a + 1
}

#lapply 
#map
 
 #question_tags2<- delete_MCAR(data.table(question_tags2)  , 0.45, "question_tags2")
 #question_tags3<- delete_MCAR(data.table(question_tags3) , 0.25, "question_tags3")

 assess_tag_dt <- data.table(question_tags2)
 View(assess_tag_dt)

 
 assess_taged_V2_new = cbind(assess_dt,assess_tag_dt[1:nrow(assess_dt),])

 # writing into file 

 # tmp.gz = tempfile(fileext = '.csv.gz')
 # gzf = gzfile(tmp.gz,'w')
 # write.csv(assess_taged_V2_new,gzf)
 # ?fwrite
 # close(gzf)
 
 system.time(fwrite(assess_taged_V2_new, "assess_taged_V2_new.csv"))


 
 # gz1 <- gzfile("assess_taged_new.gz", "w")
 # write.csv(assess_taged_new, gz1)
 # close(gz1)


 
## Processing the Streams_Tags ---> 
 
 set.seed(2500)
 cyc= 2870
 N_Blocks = 10
 a=0
 
 stream_tags_IP <- vector()
 stream_tags <- vector()
 stream_tags_IP2 <- vector()
 stream_tags2 <- vector()
 stream_tags_IP3 <- vector()
 stream_tags3 <- vector()
 
#test = sample(Tags_Master$Tags_Master,10,rep=F)
#test = data.table(test)
#ds_mcar <- delete_MCAR(test, 0.7, "test")
#ds_mcar <- delete_MCAR(test, 0.35, "test")

 
 for (a in 1:cyc) {
   stream_tags_IP <- sample(Tags_Master$Tags_Master,1134,rep=T)
   stream_tags <- append(stream_tags,stream_tags_IP)
  
   stream_tags_IP2 <- sample(Tags_Master$Tags_Master,1134,rep=T)
   #stream_tags_IP2 <- delete_MCAR(stream_tags_IP2 , 0.35, "stream_tags_IP2")
   stream_tags2 <- append(stream_tags2,stream_tags_IP2)
   
   stream_tags_IP3 <- sample(Tags_Master$Tags_Master,1134,rep=T)
   #stream_tags_IP3 <- delete_MCAR(stream_tags_IP3 , 0.7, "stream_tags_IP3")
   stream_tags3 <- append(stream_tags3,stream_tags_IP3)
   
 }
 
 views_tag_dt <- data.frame(stream_tags,stream_tags2,stream_tags3)
 views_tag_dt <- data.frame(stream_tags2,stream_tags3)
 View(views_tag_dt)
 
 views_taged_V2_new = cbind(views_dt,views_tag_dt[1:nrow(views_dt),])
 
 write.csv( views_taged_new,"C:/Users/Acer/Desktop/Sem4_ACA/FYP_TeamsStreamz/Data/views_taged_new.csv", row.names = TRUE)
 write.csv( assess_taged_new,"C:/Users/Acer/Desktop/Sem4_ACA/FYP_TeamsStreamz/Data/assess_taged_new.csv", row.names = TRUE)
 
 
 # writing into file 
 # tmp.gz = tempfile(fileext = '.csv.gz')
 # gzf = gzfile(tmp.gz,'w')
 # write.csv(views_taged_new,gzf)
 # close(gzf)
 

 #increasing the R Memory Limit 
 
 # memory.limit() 
 # memory.size()
 # #memory.limit(24000)
 # 
 # memory.size()
 # #> [1] 46.31
 # memory.size(TRUE)
 # #> [1] 48.94
 # memory.size(FALSE)
 # memory.limit()
 # #> [1] 24460
 
 
 
 
 #df.new <- views_tag_dt[sample(1:nrow(views_tag_dt), 1000000), ]
 #as.data.frame(lapply(df.new, function(cc) cc[ sample(c(TRUE, ""), prob = c(0.35, 0.75), size = length(cc), replace = TRUE) ]))
  
 
 ## Creating the Question, COuntry, Tag Master
 C_Q_Tag_M = unique(assess_dt %>% select(1,4,9))
 
 #= View(C_Q_Tag_M %>% group_by(question_id,question_tags,country) %>% summarise(count=n()))

 sereliz = C_Q_Tag_M[,2:3]
 View(sereliz )
 
 sereliz_test =  reshape(transform(sereliz, time=ave(question_tags, question_id, FUN=seq_along)), idvar="question_id", direction="wide")
 View(sereliz_test)
 Q_TAQ_SER = sereliz_test[,1:5]
 View(Q_TAQ_SER)
 
 #Country_Question_Master
 C_Q_M = unique(C_Q_Tag_M[,1:2])
 
 #Binding the Searlized Data set of Views wih counties 
 C_Q_TAQ_SER= merge(Q_TAQ_SER, C_Q_M,by = c("question_id") ) 
 #,c("country","question_id","question_tags.1","question_tags.2","question_tags.3","question_tags.4")
 
 #Converging the Data Sets across Assess_dt and views_dt for moving it out to the rest of the teams
 #1) There are Questiosn ID Duplicates in the C_Q_TAQ_SER.data.table
 C_TAQ_SER_4_VIEWDATA = unique(C_Q_TAQ_SER[,2:6])
 
 #2)Cbinding with Views Data_Table 
 
 
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
 