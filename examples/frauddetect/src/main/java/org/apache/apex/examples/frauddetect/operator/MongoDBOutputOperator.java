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
package org.apache.apex.examples.frauddetect.operator;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import com.mongodb.WriteConcern;
import com.mongodb.WriteResult;
import com.mongodb.util.JSON;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.common.util.BaseOperator;


/**
 * Operator to write data into MongoDB
 *
 * @since 0.9.0
 */
public class MongoDBOutputOperator extends BaseOperator
{
  @NotNull
  protected String hostName;
  @NotNull
  protected String dataBase;
  @NotNull
  protected String collection;

  protected WriteConcern writeConcern = WriteConcern.ACKNOWLEDGED;

  protected String userName;
  protected String passWord;

  protected transient MongoClient mongoClient;
  protected transient DB db;
  protected transient DBCollection dbCollection;

  protected List<DBObject> dataList = new ArrayList<DBObject>();

  public MongoDBOutputOperator()
  {
  }

  /**
   * Take the JSON formatted string and convert it to DBObject
   */
  public final transient DefaultInputPort<String> inputPort = new DefaultInputPort<String>()
  {
    @Override
    public void process(String tuple)
    {
      dataList.add((DBObject)JSON.parse(tuple));
    }
  };

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    try {
      mongoClient = new MongoClient(hostName);
      db = mongoClient.getDB(dataBase);
      if (userName != null && passWord != null) {
        if (!db.authenticate(userName, passWord.toCharArray())) {
          throw new IllegalArgumentException("MongoDB authentication failed. Illegal username and password for MongoDB!!");
        }
      }
      dbCollection = db.getCollection(collection);
    } catch (UnknownHostException ex) {
      logger.debug(ex.toString());
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    // nothing
  }

  @Override
  public void endWindow()
  {
    logger.debug("mongo datalist size: " + dataList.size());
    if (dataList.size() > 0) {
      WriteResult result = dbCollection.insert(dataList, writeConcern);
      logger.debug("Result for MongoDB insert: " + result);
      dataList.clear();
    }
  }

  @Override
  public void teardown()
  {
    if (mongoClient != null) {
      mongoClient.close();
    }
  }

  public String getHostName()
  {
    return hostName;
  }

  public void setHostName(String hostName)
  {
    this.hostName = hostName;
  }

  public String getDataBase()
  {
    return dataBase;
  }

  public void setDataBase(String dataBase)
  {
    this.dataBase = dataBase;
  }

  public String getCollection()
  {
    return collection;
  }

  public void setCollection(String collection)
  {
    this.collection = collection;
  }

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

  public WriteConcern getWriteConcern()
  {
    return writeConcern;
  }

  public void setWriteConcern(WriteConcern writeConcern)
  {
    this.writeConcern = writeConcern;
  }

  private static final Logger logger = LoggerFactory.getLogger(MongoDBOutputOperator.class);
}
