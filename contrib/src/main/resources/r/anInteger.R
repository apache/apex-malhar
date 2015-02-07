#!/usr/bin/Rscript
#
# This script takes two integers and returns their sum. 
# Used to test the "int' data type(s) being passed from and to the 
# Rscript operator for Malhar.
#
# num1 and num2 are passed as arguments by the client and retVal is the 
# result returned by the script.
#

anInteger <- function() {

retVal<-sum(num1, num2)
return(retVal)

}
