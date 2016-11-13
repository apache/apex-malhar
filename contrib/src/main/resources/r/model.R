#!/usr/bin/Rscript
#
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.
#


# This script receives vectors YEAR, ROLL, UNEM, HGRAD and INC as input.
# These vectors are used to construct a data frame which is then used to 
# come up with a model for unemployment based enrollments.
#
# The percentage of unemployment is being passed as input for which the 
# enrolments are to be found and returned as output.
#
# PERCENT representsthe percentage of unemployment basedon which the enrollment
# would be derived and returned.
#

 model <- function() {

 datavar = data.frame(YEAR, ROLL, UNEM, HGRAD, INC)

 #attach data variable
 attach(datavar)

 #display all data
 datavar

 #create a linear model using lm(FORMULA, DATAVAR)
 #predict the fall enrollment (ROLL) using the unemployment rate (UNEM)
 linearModelVar <- lm(ROLL ~ UNEM, datavar)

 #display linear model
 linearModelVar

 # Get the values of the intercept and unemployment so as to be able to predict the enrolment
 interc<-linearModelVar$coeff[["(Intercept)"]]
 unemp<-linearModelVar$coeff[["UNEM"]]

 # Calculate the enrollment based on teh percentage being asked for, and the model that has been reated above.
 enroll<-(interc+(unemp * PERCENT))
 retVal<-enroll
 return(retVal)
 }

