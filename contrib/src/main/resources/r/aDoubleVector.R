#!/usr/bin/Rscript

# This script takes two arraysof double numbers. It then adds these two vectors.
# It then uses these two arrays (vectors) to create metrices such that one matrix has each vector
# passed in as argument as a column and the other has the same as a row.
# The script then multiplies the two metrices  and also finds the transpose of the product.
#
# It then returns the first column as an array to the calling program.
# Used to test the Rscript operator for Malhar.
#
# num1 and num2 are passed as arguments by the client and retVal is the 
# result returned by the script.

DV <- function() {
retVal<-num1+num2

ret1<-cbind(num1, num2)
ret2<-rbind(num1, num2)
matmul<-ret1 %*% ret2
tmatmul<-t(matmul)

retVal<-tmatmul[,1]

return(as.double(retVal))
}