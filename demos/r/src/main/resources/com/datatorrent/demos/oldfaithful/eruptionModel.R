#!/usr/bin/Rscript

# This script apply the simple linear regression model for the data set 'faithful',
# and estimates the next eruption duration given the waiting time since the last eruption.
#

 eruptionModel <- function() {

 datavar = data.frame(ERUPTIONS, WAITING)

 #attach data variable
 attach(datavar)

 #create a linear model using lm(FORMULA, DATAVAR)
 #predict the fall eruption duration (ERUPT) using the waiting time since the last eruption (WAITING)
 eruption.lm <- lm(ERUPTIONS ~ WAITING, datavar)

 #display linear model
 eruption.lm

 # Get the values of the intercept and unemployment so as to be able to predict the enrolment
 interc<-eruption.lm$coeff[["(Intercept)"]]
 eruptionDuration<-eruption.lm$coeff[["WAITING"]]

 # Calculate the enrollment based on the percentage being asked for, and the model that has been rated above.
 nextEruptionDuration<-(interc+(eruptionDuration * ELAPSEDTIME))

retVal<-paste("nextEruptionDuration ", nextEruptionDuration, sep=": ")
#retVal<-c("interc : ",interc, ", eruptionDuration : ", eruptionDuration,", nextEruptionDuration : ", nextEruptionDuration)

sort( sapply(mget(ls()),object.size) )

detach(datavar);

# Clear all the data from R workspace
rm(datavar);
rm(ERUPTIONS);
rm(WAITING);

return(retVal)
}
