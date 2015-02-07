#!/usr/bin/Rscript

aBoolean <- function(){
     retVal<-c(DUMMY)
     return(retVal)
 }

 aBooleanArrayAccepted <- function(){
       retVal<-DUMMY_ARR[1:2]
       return(retVal)
   }

 aBooleanArrayReturned <- function(){
      retVal<-state.area < as.integer(AREA)
      return(retVal)
  }