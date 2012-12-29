/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.mongodb;

import com.malhartech.annotation.InputPortFieldAnnotation;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultInputPort;
import com.malhartech.api.Operator;
import com.mongodb.*;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.logging.Level;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class MongoDBOutputOperator<T> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBOutputOperator.class);
  private static final int DEFAULT_BATCH_SIZE = 1000;
  @NotNull
  private String dbUrl;
  private String dataBase;
  @Min(1)
  private long batchSize = DEFAULT_BATCH_SIZE;
  private String userName;
  private String passWord;
  private transient MongoClient mongoClient;
  protected transient DB db;
  protected transient HashMap<String, String> propTable = new HashMap<String, String>();  // prop-table mapping for HashMap
  protected transient ArrayList<String> tableNames = new ArrayList<String>();
  private String maxWindowTable;
  protected transient DBCollection maxWindowCollection;
  protected transient long windowId;
  protected transient String operatorId;
//  protected transient String applicationId;
  protected transient long lastWindowId;
  protected transient boolean ignoreWindow;
  protected transient int tupleId;
  protected String windowIdName;
  protected String operatorIdName;
//  protected String applicationIdName;

  /**
   * Implement how to process tuple in derived class based on HashMap or ArrayList.
   * The tuple values are binded with SQL prepared statement to be inserted to database.
   *
   * @param tuple
   * @throws SQLException
   */
  public abstract void processTuple(T tuple);
  /**
   * The input port.
   */
  @InputPortFieldAnnotation(name = "inputPort")
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>(this)
  {
    @Override
    public void process(T tuple)
    {
      if (ignoreWindow) {
        return; // ignore
      }

      try {
        processTuple(tuple);

      }
      catch (Exception ex) {
        throw new RuntimeException("Exception during process tuple", ex);
      }
    }
  };

  public void initLastWindowInfo()
  {
    maxWindowCollection = db.getCollection(maxWindowTable);
    BasicDBObject query = new BasicDBObject();
    query.put(operatorIdName, operatorId);
//    query.put(applicationIdName, "0");
    DBCursor cursor = maxWindowCollection.find();
    if (cursor.hasNext()) {
      Object obj = cursor.next().get(windowIdName);
      lastWindowId = (Long)obj;
    }
    else {
      BasicDBObject doc = new BasicDBObject();
      doc.put(windowIdName, (long)0);
//      doc.put(applicationIdName, 0);
      doc.put(operatorIdName, operatorId);
      maxWindowCollection.save(doc);
    }

    System.out.println("last windowid:" + lastWindowId);
  }

  /**
   * Implement Operator Interface.
   */
  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    tupleId = 0;
    if (windowId < lastWindowId) {
      ignoreWindow = true;
    }
    else if (windowId == lastWindowId) {
      ignoreWindow = false;
      BasicDBObject query = new BasicDBObject();
//      query.put(windowIdName, windowId);
      query.put(operatorIdName, operatorId);
      ByteBuffer bb = ByteBuffer.allocate(12);
      bb.putLong(windowId);
      bb.putInt(0);
      StringBuilder low = new StringBuilder();
      for (byte b : bb.array()) {
        low.append(String.format("02x", b&0xff));
      }

      query.put("_id", new BasicDBObject("$gte",new ObjectId(low.toString())));
//      query.put(applicationIdName, 0);
      for (String table : tableNames) {
        db.getCollection(table).remove(query);
      }
    }
    else {
      ignoreWindow = false;
    }
  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
    operatorId = context.getId();
    try {
      mongoClient = new MongoClient(dbUrl);
      db = mongoClient.getDB(dataBase);
      if (userName != null && passWord != null) {
        db.authenticate(userName, passWord.toCharArray());
      }
      initLastWindowInfo();
    }
    catch (UnknownHostException ex) {
      logger.debug(ex.toString());
    }
  }

  public void buildMapping()
  {
  }

  @Override
  public void teardown()
  {
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

  public void addTable(String table)
  {
    tableNames.add(table);
  }

  public ArrayList<String> getTableNames()
  {
    return tableNames;
  }

  public long getBatchSize()
  {
    return batchSize;
  }

  public void setBatchSize(long batchSize)
  {
    this.batchSize = batchSize;
  }

  public String getMaxWindowTable()
  {
    return maxWindowTable;
  }

  public void setMaxWindowTable(String maxWindowTable)
  {
    this.maxWindowTable = maxWindowTable;
  }

  public String getWindowIdName()
  {
    return windowIdName;
  }

  public void setWindowIdName(String windowIdName)
  {
    this.windowIdName = windowIdName;
  }

  public String getOperatorIdName()
  {
    return operatorIdName;
  }

  public void setOperatorIdName(String operatorIdName)
  {
    this.operatorIdName = operatorIdName;
  }

//  public String getApplicationIdName()
//  {
//    return applicationIdName;
//  }
//
//  public void setApplicationIdName(String applicationIdName)
//  {
//    this.applicationIdName = applicationIdName;
//  }
  public String getDataBase()
  {
    return dataBase;
  }

  public void setDataBase(String dataBase)
  {
    this.dataBase = dataBase;
  }

  public String getDbUrl()
  {
    return dbUrl;
  }

  public void setDbUrl(String dbUrl)
  {
    this.dbUrl = dbUrl;
  }

  public long getLastWindowId()
  {
    return lastWindowId;
  }

  public void setLastWindowId(long lastWindowId)
  {
    this.lastWindowId = lastWindowId;
  }
}
