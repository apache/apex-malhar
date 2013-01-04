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
import java.nio.ByteOrder;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.logging.Level;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoDB output adapter operator, which send insertion data to nontransaction database.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port <br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>hostName</b>:the host name of the database to connect to<br>
 * <b>database</b>:the database to connect to <br>
 * <b></b>: <br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * Nontransaction database requires additional operatorId, windowId, applicationId column,<br>
 * to store the last committed windowId information for recovery purpose<br>
 * user needs to create the additional columns and assign the column names as windowIdColumnName,operatorIdColumnName,applicationIdColumnName<br>
 * <br>
 * <b>Benchmarks</b>:
 * <br>
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public abstract class MongoDBOutputOperator<T> implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBOutputOperator.class);
  private static final int DEFAULT_BATCH_SIZE = 1000;
  @NotNull
  private String hostName;
  private String dataBase;
  @Min(1)
  protected long batchSize = DEFAULT_BATCH_SIZE;
  private String userName;
  private String passWord;
  private transient MongoClient mongoClient;
  protected transient DB db;
  protected transient HashMap<String, String> propTableMap = new HashMap<String, String>();  // prop-table mapping for HashMap
  protected transient ArrayList<String> tableList = new ArrayList<String>();
  protected transient HashMap<String, List<DBObject>> tableDocumentList = new HashMap<String, List<DBObject>>();
  private String maxWindowTable;
  protected transient DBCollection maxWindowCollection;
  protected transient long windowId;
  protected transient String operatorId;
//  protected transient String applicationId;
  protected transient long lastWindowId;
  protected transient boolean ignoreWindow;
  protected transient int tupleId;
  protected String windowIdColumnName;
  protected String operatorIdColumnName;
//  protected String applicationIdName;
  protected int queryFunction;

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
    query.put(operatorIdColumnName, operatorId);
//    query.put(applicationIdName, "0");
    DBCursor cursor = maxWindowCollection.find(query);
    if (cursor.hasNext()) {
      Object obj = cursor.next().get(windowIdColumnName);
      lastWindowId = (Long)obj;
    }
    else {
      BasicDBObject doc = new BasicDBObject();
      doc.put(windowIdColumnName, (long)0);
//      doc.put(applicationIdName, 0);
      doc.put(operatorIdColumnName, operatorId);
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
    tupleId = 1;
    if (windowId < lastWindowId) {
      ignoreWindow = true;
    }
    else if (windowId == lastWindowId) {
      ignoreWindow = false;
      BasicDBObject query = new BasicDBObject();
//      query.put(windowIdColumnName, windowId);
//      query.put(operatorIdColumnName, operatorId);
      ByteBuffer bb = ByteBuffer.allocate(12);
      bb.order(ByteOrder.BIG_ENDIAN);
      StringBuilder low = new StringBuilder();
      StringBuilder high = new StringBuilder();
      if (queryFunction == 1) {
        queryFunction1(bb, high, low);
      }
      else if (queryFunction == 2) {
        queryFunction2(bb, high, low);
      }
      else if (queryFunction == 3) {
        queryFunction3(bb, high, low);
      }
      else {
        throw new RuntimeException("unknown queryFunction type:" + queryFunction);
      }

      query.put("_id", new BasicDBObject("$gte", new ObjectId(low.toString())).append("$lte", new ObjectId(high.toString())));
//      query.put(applicationIdName, 0);
      for (String table : tableList) {
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
    if (ignoreWindow) {
      return;
    }
    
    for( String table : tableList ) {
      List<DBObject> docList = tableDocumentList.get(table);
      db.getCollection(table).insert(docList);
    }
  }

  @Override
  public void setup(OperatorContext context)
  {
    operatorId = context.getId();
    try {
      mongoClient = new MongoClient(hostName);
      db = mongoClient.getDB(dataBase);
      if (userName != null && passWord != null) {
        db.authenticate(userName, passWord.toCharArray());
      }
      initLastWindowInfo();
      buildMapping();
    }
    catch (UnknownHostException ex) {
      logger.debug(ex.toString());
    }
  }

  public void buildMapping()
  {
    for (String table : tableList) {
      tableDocumentList.put(table, new ArrayList<DBObject>());
    }
  }

  @Override
  public void teardown()
  {
  }

  /* 8B windowId | 1B opratorId | 3B tupleId */
  public void queryFunction1(ByteBuffer bb, StringBuilder high, StringBuilder low)
  {
    bb.putLong(windowId);
    byte opId = (byte)Integer.parseInt(operatorId);
    bb.put(opId);
    ByteBuffer lowbb = bb;
    lowbb.put((byte)0);
    lowbb.put((byte)0);
    lowbb.put((byte)0);
//    String str = Hex.encodeHexString(lowbb.array());
    for (byte b : lowbb.array()) {
      low.append(String.format("02x", b & 0xff));
    }

    ByteBuffer highbb = bb;
    highbb.put((byte)0xff);
    highbb.put((byte)0xff);
    highbb.put((byte)0xff);
    for (byte b : highbb.array()) {
      high.append(String.format("02x", b & 0xff));
    }
  }

  /* 4B baseSec | 2B windowId | 3B operatorId | 3B tupleId */
  public void queryFunction2(ByteBuffer bb, StringBuilder high, StringBuilder low)
  {
    int baseSec = (int)(windowId >> 32);
    bb.putInt(baseSec);
    short winId = (short)(windowId & 0xffff);
    bb.putShort(winId);
    Integer operId = Integer.parseInt(operatorId);
    for (int i = 0; i < 3; i++) {
      byte num = (byte)(operId >> 8 * (2 - i));
      bb.put(num);
    }
    ByteBuffer lowbb = bb.duplicate();
    lowbb.put((byte)0);
    lowbb.put((byte)0);
    lowbb.put((byte)0);
    for (byte b : lowbb.array()) {
      low.append(String.format("%02x", b & 0xff));
    }

    ByteBuffer highbb = bb.duplicate();
    highbb.put((byte)0xff);
    highbb.put((byte)0xff);
    highbb.put((byte)0xff);
    for (byte b : highbb.array()) {
      high.append(String.format("%02x", b & 0xff));
    }
  }

  /* 4B baseSec | 3B operatorId | 2B windowId | 3B tupleId */
  public void queryFunction3(ByteBuffer bb, StringBuilder high, StringBuilder low)
  {
    int baseSec = (int)(windowId >> 32);
    bb.putInt(baseSec);
    Integer operId = Integer.parseInt(operatorId);
    for (int i = 0; i < 3; i++) {
      byte num = (byte)(operId >> 8 * (2 - i));
      bb.put(num);
    }
    short winId = (short)(windowId & 0xffff);
    bb.putShort(winId);

    ByteBuffer lowbb = bb.duplicate();
    lowbb.put((byte)0);
    lowbb.put((byte)0);
    lowbb.put((byte)0);
    for (byte b : lowbb.array()) {
      low.append(String.format("%02x", b & 0xff));
    }
    ByteBuffer highbb = bb.duplicate();
    highbb.put((byte)0xff);
    highbb.put((byte)0xff);
    highbb.put((byte)0xff);
    for (byte b : highbb.array()) {
      high.append(String.format("%02x", b & 0xff));
    }
  }

  /*  8B windowId | 1B operatorId | 3B tupleId */
  void insertFunction1(ByteBuffer bb)
  {
    bb.putLong(windowId);
    byte oid = (byte)Integer.parseInt(operatorId);
    bb.put(oid);
    for (int i = 0; i < 3; i++) {
      byte num = (byte)(tupleId >> 8 * (2 - i));
      bb.put(num);
    }
  }

  /* 4B baseSec | 3B operatorId | 2B windowId | 3B tupleId */
  void insertFunction2(ByteBuffer bb)
  {
    int baseSec = (int)(windowId >> 32);
    bb.putInt(baseSec);
    Integer operId = Integer.parseInt(operatorId);
    for (int i = 0; i < 3; i++) {
      byte num = (byte)(operId >> 8 * (2 - i));
      bb.put(num);
    }
    bb.putShort((short)(windowId & 0xffff));
    for (int i = 0; i < 3; i++) {
      byte num = (byte)(tupleId >> 8 * (2 - i));
      bb.put(num);
    }
  }

  /* 4B baseSec | 2B windowId | 3B operatorId | 3B tupleId */
  void insertFunction3(ByteBuffer bb)
  {
    int baseSec = (int)(windowId >> 32);
    bb.putInt(baseSec);
    short winId = (short)(windowId & 0xffff);
    bb.putShort(winId);
    Integer operId = Integer.parseInt(operatorId);
    for (int i = 0; i < 3; i++) {
      byte num = (byte)(operId >> 8 * (2 - i));
      bb.put(num);
    }
    for (int i = 0; i < 3; i++) {
      byte num = (byte)(tupleId >> 8 * (2 - i));
      bb.put(num);
    }
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
    tableList.add(table);
  }

  public ArrayList<String> getTableList()
  {
    return tableList;
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

  public String getWindowIdColumnName()
  {
    return windowIdColumnName;
  }

  public void setWindowIdColumnName(String windowIdColumnName)
  {
    this.windowIdColumnName = windowIdColumnName;
  }

  public String getOperatorIdColumnName()
  {
    return operatorIdColumnName;
  }

  public void setOperatorIdColumnName(String operatorIdColumnName)
  {
    this.operatorIdColumnName = operatorIdColumnName;
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

  public String getHostName()
  {
    return hostName;
  }

  public void setHostName(String dbUrl)
  {
    this.hostName = dbUrl;
  }

  public long getLastWindowId()
  {
    return lastWindowId;
  }

  public void setLastWindowId(long lastWindowId)
  {
    this.lastWindowId = lastWindowId;
  }

  public void setQueryFunction(int queryFunction)
  {
    this.queryFunction = queryFunction;
  }
}
