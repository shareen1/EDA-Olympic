library('tesseract')
image.Data<-ocr("C:\\Users\\vmaji\\OneDrive\\Desktop\\PDE_PRO\\test.jpg", engine = tesseract("eng"))
s1<-gsub("[[:punct:]]", "", image.Data)
image.Array<-strsplit(s1,'\n')
image.realData<-image.Array[[1]]
image.realData[4]<-"59 59 56 59 48"
image.realData[5]<-"40 41 43 41 51"

POSITIVE<-image.realData[4]
NEGATIVE<-image.realData[5]
YEARS<-image.realData[9]
POSITIVE_DATA<-strsplit(POSITIVE,' ')
NEGATIVE_DATA<-strsplit(NEGATIVE,' ')
YEARS_DATA<-strsplit(YEARS,' ')
negativecol<-NULL
for( i in 1:length(NEGATIVE_DATA[[1]])){
  n_data<-NEGATIVE_DATA[[1]][i]
  negativecol<-c(n_data,negativecol)
}
Positivecol<-NULL
for( i in 1:length(POSITIVE_DATA[[1]])){
  c_data<-POSITIVE_DATA[[1]][i]
  Positivecol<-c(c_data,Positivecol)
}
yearcol<-NULL
for( i in 1:length(YEARS_DATA[[1]])){
  my_data<-YEARS_DATA[[1]][i]
  yearcol<-c(my_data,yearcol)
}
datafile<-data.frame(yearcol,Positivecol,negativecol)
write.csv(datafile,"C:\\Users\\vmaji\\OneDrive\\Desktop\\PDE_PRO\\Olympic_servey.csv",row.names = FALSE)

