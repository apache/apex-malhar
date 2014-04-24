/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.mongodb;

import java.net.UnknownHostException;

import javax.validation.constraints.NotNull;

import com.mongodb.*;

import com.datatorrent.lib.db.Connectable;

/**
 * MongoDB base operator, which has basic information for an i/o operator.<p><br>
 *
 * <br>
 * Properties:<br>
 * <b>hostName</b>:the host name of the database to connect to, not null<br>
 * <b>dataBase</b>:the database to connect to<br>
 * <b>userName</b>:userName for connection to database<br>
 * <b>passWord</b>:password for connection to database<br>
 * <b>mongoClient</b>:created when connected to database<br>
 * <b>db</b>:created when connected to database<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * hostName
 * batchSize <br>
 * <b>data type:</br>the insertion data can support all the Objects mongoDB supports<br>
 *
 * <b>Benchmarks</b>:
 * <br>
 *
 * @since 0.3.2
 */
public class MongoDBConnectable implements Connectable
{
  @NotNull
  protected String hostName;
  protected String dataBase;

  protected String userName;
  protected String passWord;
  protected transient MongoClient mongoClient;
  protected transient DB db;

  public String getUserName()
  {
    return userName;
  }

  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public String getPassWord()
  {
    return passWord;
  }

  public void setPassWord(String passWord)
  {
    this.passWord = passWord;
  }

  public String getDataBase()
  {
    return dataBase;
  }

  public void setDataBase(String dataBase)
  {
    this.dataBase = dataBase;
  }

  public String getHostName()
  {
    return hostName;
  }

  public void setHostName(String dbUrl)
  {
    this.hostName = dbUrl;
  }

  @Override
  public void connect()
  {
    try {
      mongoClient = new MongoClient(hostName);
      db = mongoClient.getDB(dataBase);
      if (userName != null && passWord != null) {
        db.authenticate(userName, passWord.toCharArray());
      }
    }
    catch (UnknownHostException ex) {
      throw new RuntimeException("creating mongodb client", ex);
    }
  }

  @Override
  public void disconnect()
  {
    mongoClient.close();
  }

  @Override
  public boolean connected()
  {
    try {
      mongoClient.getConnector().getDBPortPool(mongoClient.getAddress()).get().ensureOpen();
    }
    catch (Exception ex) {
      return false;
    }
    return true;
  }
}
