df<-read.csv('C:/Users/vmaji/OneDrive/Desktop/PDE_PRO/athlete_events.csv', header = TRUE, stringsAsFactors = FALSE)
df1<-read.csv('C:/Users/vmaji/OneDrive/Desktop/PDE_PRO/noc_regions.csv', header = TRUE, stringsAsFactors = FALSE)

df<-df[-14]
df<-df[-9]
df<-df[-7]
colSums(is.na(df))
Height1 <- replace(df$Height,is.na(df$Height),0)
mean_Height1<-median(as.integer(Height1))
df$Height <- replace(df$Height,is.na(df$Height),mean_Height1)
Weight1 <- replace(df$Weight,is.na(df$Weight),0)
mean_Weight1<-median(as.integer(Weight1))
df$Weight <- replace(df$Weight,is.na(df$Weight),mean_Weight1)
Age1 <- replace(df$Age,is.na(df$Age),0)
mean_age<-median(as.integer(Age1))
df$Age <- replace(df$Age,is.na(df$Age),mean_age)
df$Medal <- as.character(df$Medal)
df$Medal <- replace(df$Medal,is.na(df$Medal),'participant')
colSums(is.na(df))
df$uniqeID<-c(1:nrow(df))
write.csv(df,"C:/Users/vmaji/OneDrive/Desktop/PDE_PRO/olymbics.csv" ,row.names = FALSE)