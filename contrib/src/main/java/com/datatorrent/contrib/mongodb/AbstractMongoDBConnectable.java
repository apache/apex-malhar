/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.mongodb;

import java.io.IOException;
import java.net.UnknownHostException;

import javax.validation.constraints.NotNull;

import com.mongodb.DB;
import com.mongodb.MongoClient;

import com.datatorrent.lib.db.Connectable;

/**
 * Connectable implementation for MongoDB
 */
public abstract class AbstractMongoDBConnectable implements Connectable
{
  @NotNull
  protected String host;
  protected int port;
  protected String database;
  protected String userName;
  protected String password;
  protected transient MongoClient mongoClient;
  protected transient DB db;

  @Override
  public void connect() throws IOException
  {
    try {
      if (port > 0) {
        mongoClient = new MongoClient(host + ":" + port);
      } else {
        // default port
        mongoClient = new MongoClient(host);
      }
      db = mongoClient.getDB(database);
      if (userName != null && password != null) {
        db.authenticate(userName, password.toCharArray());
      }
    }
    catch (UnknownHostException ex) {
      throw new RuntimeException("creating mongodb client", ex);
    }

  }

  @Override
  public void disconnect() throws IOException
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

  /**
   * host name of MongoDB
   *
   * @return host name
   */
  public String getHostName()
  {
    return host;
  }

  /**
   * host name of MongoDB
   *
   * @param host host name
   */
  public void setHost(String host)
  {
    this.host = host;
  }

  /**
   * port for MongoDB server
   * @return port
   */
  public int getPort()
  {
    return port;
  }

  /**
   * port for MongoDB server
   * @param port port
   */
  public void setPort(int port)
  {
    this.port = port;
  }


  /**
   * name of the database
   *
   * @return database
   */
  public String getDatabase()
  {
    return database;
  }

  /**
   * name of the database
   *
   * @param database database
   */
  public void setDatabase(String database)
  {
    this.database = database;
  }

  /**
   * user name
   *
   * @return user name
   */
  public String getUserName()
  {
    return userName;
  }

  /**
   * user name
   *
   * @param userName user name
   */
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  /**
   * password
   *
   * @param password password
   */
  public void setPassword(String password)
  {
    this.password = password;
  }

}
