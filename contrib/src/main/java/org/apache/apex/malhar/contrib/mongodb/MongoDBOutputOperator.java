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
package org.apache.apex.malhar.contrib.mongodb;

import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.validation.constraints.Min;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.Operator;

/**
 * This is the base implementation for a non transactional output operator for MongoDB.&nbsp;
 * Subclasses should implement the column mapping for writing tuples out to MongoDB.
 * <p>
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port <br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>maxWindowTable</b>:the table to save the most recent inserted windowId, operatorId information for recovery use<br>
 * <b>maxWindowCollection</b>:mongoDB collection of the maxWindowTable<br>
 * <b>windowId</b>:Id of current window<br>
 * <b>operatorId</b>:Id of the operator<br>
 * <b>batchSize</b>:size for each batch insert, default value is 1000<br>
 * <b>lastWindowId</b>:last inserted windowId, is obtained at setup from maxWindowTable with specific operatorId<br>
 * <b>ignoreWindow</b>:the flag to indicate ignoring out of date window <br>
 * <b>tupleId</b>:the Id of the tuple, incrementing at each tuple process, start from 1 at beginWindow()<br>
 * <b>windowIdColumnName</b>:the name of the windowId column in maxWindowTable, should be set by the user<br>
 * <b>operatorIdColumnName</b>:the name of the operatorId column in maxWindowTable, should be set by the user<br>
 * <b>tableList</b>: the list of all the tables of the mapping<br>
 * <b>tableToDocument</b>:each tuple corresponds to one document for one collection to be inserted<br>
 * <b>tableToDocumentList</b>:for bulk insert, each table has a document list to insert. This is table and document list map <br>
 * <b>tupleId</b>:the Id of the tuple, incrementing at each tuple process, start from 1 at beginWindow()<br>
 * <b>queryFunction</b>:corresponding to the option for the ObjectId of 12 bytes format saving. The windowId, tupleId, operatorId of each tuple are saved in each collection as the column ObjectId for recovery<br>
 * It Currently has 3 format for the ObjectId. When the operator recovers, it will remove the document which has the same windowId, operatorId as maxWindowTable in the collections, and insert the documents again<br>
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
 * </p>
 * @displayName MongoDB Output
 * @category Output
 * @tags mongodb
 * @since 0.3.2
 */
public abstract class MongoDBOutputOperator<T> extends MongoDBConnectable implements Operator
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBOutputOperator.class);
  protected static final int DEFAULT_BATCH_SIZE = 1000;
  @Min(1)
  protected long batchSize = DEFAULT_BATCH_SIZE;
  protected transient ArrayList<String> tableList = new ArrayList<String>(); // all the tables in the mapping
  protected transient HashMap<String, BasicDBObject> tableToDocument = new HashMap<String, BasicDBObject>(); // each table has one document to insert
  protected transient HashMap<String, List<DBObject>> tableToDocumentList = new HashMap<String, List<DBObject>>();
  protected String maxWindowTable;
  protected transient DBCollection maxWindowCollection;
  protected transient long windowId;
  protected transient int operatorId;
//  protected transient String applicationId;
  protected transient long lastWindowId;
  protected transient boolean ignoreWindow;
  protected String windowIdColumnName;
  protected String operatorIdColumnName;
  protected transient int tupleId;
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
   * This input port receives tuples which will be written to MongoDB.
   */
  public final transient DefaultInputPort<T> inputPort = new DefaultInputPort<T>()
  {
    @Override
    public void process(T tuple)
    {
      if (ignoreWindow) {
        return; // ignore
      }

      try {
        processTuple(tuple);
      } catch (Exception ex) {
        throw new RuntimeException("Exception during process tuple", ex);
      }
    }
  };

  /**
   * init last completed windowId information with operatorId, read from maxWindowTable.
   * If the table is empty, insert a default value document
   */
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
    } else {
      BasicDBObject doc = new BasicDBObject();
      doc.put(windowIdColumnName, (long)0);
//      doc.put(applicationIdName, 0);
      doc.put(operatorIdColumnName, operatorId);
      maxWindowCollection.save(doc);
    }

    logger.debug("last windowid: {}", lastWindowId);
  }

  /**
   * Implement Operator Interface.
   * If windowId is less than the last completed windowId, then ignore the window.
   * If windowId is equal to the last completed windowId, then remove the documents with same windowId of the operatorId, and insert the documents later
   * If windowId is greater then the last completed windowId, then process the window
   *
   * @param windowId
   */
  @Override
  public void beginWindow(long windowId)
  {
    this.windowId = windowId;
    tupleId = 1;
    if (windowId < lastWindowId) {
      ignoreWindow = true;
    } else if (windowId == lastWindowId) {
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
      } else if (queryFunction == 2) {
        queryFunction2(bb, high, low);
      } else if (queryFunction == 3) {
        queryFunction3(bb, high, low);
      } else {
        throw new RuntimeException("unknown queryFunction type:" + queryFunction);
      }

      query.put("_id", new BasicDBObject("$gte", new ObjectId(low.toString())).append("$lte", new ObjectId(high.toString())));
//      query.put(applicationIdName, 0);
      for (String table : tableList) {
        db.getCollection(table).remove(query);
      }
    } else {
      ignoreWindow = false;
    }
  }

  /**
   * At endWindow, if not ignoring window, then insert bulk document list
   */
  @Override
  public void endWindow()
  {
    if (ignoreWindow) {
      return;
    }

    BasicDBObject where = new BasicDBObject(); // update maxWindowTable for windowId information
    where.put(operatorIdColumnName, operatorId);
    BasicDBObject value = new BasicDBObject();
    value.put(operatorIdColumnName, operatorId);
    value.put(windowIdColumnName, windowId);
    maxWindowCollection.update(where, value);

    for (String table : tableList) {
      List<DBObject> docList = tableToDocumentList.get(table);
      db.getCollection(table).insert(docList);
    }
  }

  /**
   * At setup time, init last completed windowId from maxWindowTable
   *
   * @param context
   */
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
      for (String table : tableList) {
        tableToDocumentList.put(table, new ArrayList<DBObject>());
        tableToDocument.put(table, new BasicDBObject());
      }
    } catch (UnknownHostException ex) {
      logger.debug(ex.toString());
    }
  }

  public abstract void setColumnMapping(String[] mapping);

  @Override
  public void teardown()
  {
  }

  /**
   * shared processTuple for HashMap and ArrayList output Operator.
   */
  public void processTupleCommon()
  {
    ByteBuffer bb = ByteBuffer.allocate(12);
    bb.order(ByteOrder.BIG_ENDIAN);
    if (queryFunction == 1) {
      insertFunction1(bb);
    } else if (queryFunction == 2) {
      insertFunction2(bb);
    } else if (queryFunction == 3) {
      insertFunction3(bb);
    } else {
      throw new RuntimeException("unknown insertFunction type:" + queryFunction);
    }
//    String str = Hex.encodeHexString(bb.array());
    StringBuilder objStr = new StringBuilder();
    for (byte b : bb.array()) {
      objStr.append(String.format("%02x", b & 0xff));
    }

    BasicDBObject doc = null;
    for (Map.Entry<String, BasicDBObject> entry : tableToDocument.entrySet()) {
      String table = entry.getKey();
      doc = entry.getValue();
      doc.put("_id", new ObjectId(objStr.toString()));
      List<DBObject> docList = tableToDocumentList.get(table);
      docList.add(doc);
      if (tupleId % batchSize == 0) { // do batch insert here
        BasicDBObject where = new BasicDBObject(); // update maxWindowTable for windowId information
        where.put(operatorIdColumnName, operatorId);
        BasicDBObject value = new BasicDBObject();
        value.put(operatorIdColumnName, operatorId);
        value.put(windowIdColumnName, windowId);
        maxWindowCollection.update(where, value);

        db.getCollection(table).insert(docList);
        tableToDocumentList.put(table, new ArrayList<DBObject>());
      } else {
        tableToDocumentList.put(table, docList);
      }
    }
    ++tupleId;
  }

  /**
   * 8B windowId | 1B opratorId | 3B tupleId
   */
  public void queryFunction1(ByteBuffer bb, StringBuilder high, StringBuilder low)
  {
    bb.putLong(windowId);
    byte opId = (byte)(operatorId);
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

  /**
   * 4B baseSec | 2B windowId | 3B operatorId | 3B tupleId
   */
  public void queryFunction2(ByteBuffer bb, StringBuilder high, StringBuilder low)
  {
    int baseSec = (int)(windowId >> 32);
    bb.putInt(baseSec);
    short winId = (short)(windowId & 0xffff);
    bb.putShort(winId);
    Integer operId = operatorId;
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

  /**
   * 4B baseSec | 3B operatorId | 2B windowId | 3B tupleId
   */
  public void queryFunction3(ByteBuffer bb, StringBuilder high, StringBuilder low)
  {
    int baseSec = (int)(windowId >> 32);
    bb.putInt(baseSec);
    Integer operId = operatorId;
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

  /**
   * 8B windowId | 1B operatorId | 3B tupleId
   */
  void insertFunction1(ByteBuffer bb)
  {
    bb.putLong(windowId);
    byte oid = (byte)(operatorId);
    bb.put(oid);
    for (int i = 0; i < 3; i++) {
      byte num = (byte)(tupleId >> 8 * (2 - i));
      bb.put(num);
    }
  }

  /**
   * 4B baseSec | 3B operatorId | 2B windowId | 3B tupleId
   */
  void insertFunction2(ByteBuffer bb)
  {
    int baseSec = (int)(windowId >> 32);
    bb.putInt(baseSec);
    Integer operId = operatorId;
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

  /**
   * 4B baseSec | 2B windowId | 3B operatorId | 3B tupleId
   */
  void insertFunction3(ByteBuffer bb)
  {
    int baseSec = (int)(windowId >> 32);
    bb.putInt(baseSec);
    short winId = (short)(windowId & 0xffff);
    bb.putShort(winId);
    Integer operId = operatorId;
    for (int i = 0; i < 3; i++) {
      byte num = (byte)(operId >> 8 * (2 - i));
      bb.put(num);
    }
    for (int i = 0; i < 3; i++) {
      byte num = (byte)(tupleId >> 8 * (2 - i));
      bb.put(num);
    }
  }

  public void addTable(String table)
  {
    tableList.add(table);
  }

  public ArrayList<String> getTableList()
  {
    return tableList;
  }

  public void setQueryFunction(int queryFunction)
  {
    this.queryFunction = queryFunction;
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

  public long getLastWindowId()
  {
    return lastWindowId;
  }

  public void setLastWindowId(long lastWindowId)
  {
    this.lastWindowId = lastWindowId;
  }
}
