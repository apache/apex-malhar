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