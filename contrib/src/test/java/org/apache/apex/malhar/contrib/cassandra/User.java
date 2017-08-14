/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.cassandra;

import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class User
{
  private String userid;
  private FullName username;
  private Set<String> emails;
  private List<Integer> topScores;
  private Map<Date, String> todo;
  private Address currentaddress;
  private List<FullName> previousnames;

  private String nonMatchingColumn;

  private List<Integer> nonMatchingCollectionColumn;

  public String getUserid()
  {
    return userid;
  }

  public void setUserid(String userid)
  {
    this.userid = userid;
  }

  public FullName getUsername()
  {
    return username;
  }

  public void setUsername(FullName username)
  {
    this.username = username;
  }

  public Set<String> getEmails()
  {
    return emails;
  }

  public void setEmails(Set<String> emails)
  {
    this.emails = emails;
  }

  public List<Integer> getTopScores()
  {
    return topScores;
  }

  public void setTopScores(List<Integer> topScores)
  {
    this.topScores = topScores;
  }

  public Map<Date, String> getTodo()
  {
    return todo;
  }

  public void setTodo(Map<Date, String> todo)
  {
    this.todo = todo;
  }

  public String getNonMatchingColumn()
  {
    return nonMatchingColumn;
  }

  public void setNonMatchingColumn(String nonMatchingColumn)
  {
    this.nonMatchingColumn = nonMatchingColumn;
  }

  public List<Integer> getNonMatchingCollectionColumn()
  {
    return nonMatchingCollectionColumn;
  }

  public void setNonMatchingCollectionColumn(List<Integer> nonMatchingCollectionColumn)
  {
    this.nonMatchingCollectionColumn = nonMatchingCollectionColumn;
  }

  public Address getCurrentaddress()
  {
    return currentaddress;
  }

  public void setCurrentaddress(Address currentaddress)
  {
    this.currentaddress = currentaddress;
  }

  public List<FullName> getPreviousnames()
  {
    return previousnames;
  }

  public void setPreviousnames(List<FullName> previousnames)
  {
    this.previousnames = previousnames;
  }
}
