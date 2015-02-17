#!/usr/bin/Rscript

# This script takes two real numbers and returns their sum as a double.
#
# Used to test the "real' data type(s) being passed from and to the 
# Rscript operator for Malhar.
#
# num1 and num2 are passed as arguments by the client and retVal is the 
# result returned by the script.

aReal <- function(){

retVal<-sum(num1, num2)
return(as.double(retVal))

}