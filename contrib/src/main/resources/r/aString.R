#!/usr/bin/Rscript
#`
# This script takes two strings and a seperator and concatenates the two strings seperated  by the seperator.
# It returns such a newly formed string
#
# Used to test the "string' data type(s) being passed from and to the Rscript operator for Malhar.
# str1 and str2 are passed as arguments by the client and retVal is the result returned by the script.
#

aString <- function(){
retVal<-c("abc")

print(retVal)

f<-function(a, b, c) {
    result<-paste(a, b, sep=c)
    return(result)
}

retVal<-f(str1, str2, seperator)
return(retVal)
}