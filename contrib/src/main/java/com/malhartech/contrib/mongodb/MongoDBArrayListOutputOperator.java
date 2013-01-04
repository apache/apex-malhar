/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.mongodb;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MongoDBArrayListOutputOperator extends MongoDBOutputOperator<ArrayList<Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBArrayListOutputOperator.class);
  private transient ArrayList<String> propList = new ArrayList<String>();
  protected transient ArrayList<String> tableMapping = new ArrayList<String>();

  public void addProp(String prop)
  {
    propList.add(prop);
  }

  public void addTableMapping(String table)
  {
    tableMapping.add(table);
  }

  @Override
  public void processTuple(ArrayList<Object> tuple)
  {
    if (windowId > lastWindowId) {
      lastWindowId = windowId;
      BasicDBObject where = new BasicDBObject();
//      doc1.put(applicationIdName, 0);
      where.put(operatorIdColumnName, operatorId);
      BasicDBObject value = new BasicDBObject();
//      doc2.put(applicationIdName, 0);
      value.put(operatorIdColumnName, operatorId);
      value.put(windowIdColumnName, windowId);
      maxWindowCollection.update(where, value);
    }

    BasicDBObject doc = null;
    HashMap<String, BasicDBObject> tableDoc = new HashMap<String, BasicDBObject>(); // each table has one document to insert

    for (int i = 0; i < tuple.size(); i++) {
      String table = tableMapping.get(i);
      if ((doc = tableDoc.get(table)) == null) {
        doc = new BasicDBObject();
        doc.put(propList.get(i), tuple.get(i));
      }
      else {
        doc.put(propList.get(i), tuple.get(i));
      }
      tableDoc.put(table, doc);
    }

    ByteBuffer bb = ByteBuffer.allocate(12);
    bb.order(ByteOrder.BIG_ENDIAN);
    if (queryFunction == 1) {
      insertFunction1(bb);
    }
    else if (queryFunction == 2) {
      insertFunction2(bb);
    }
    else if (queryFunction == 3) {
      insertFunction3(bb);
    }
    else {
      throw new RuntimeException("unknown insertFunction type:" + queryFunction);
    }
//    String str = Hex.encodeHexString(bb.array());
    StringBuilder sb = new StringBuilder();
    for (byte b : bb.array()) {
      sb.append(String.format("%02x", b & 0xff));
    }

    for (Map.Entry<String, BasicDBObject> entry : tableDoc.entrySet()) {
      String table = entry.getKey();
      doc = entry.getValue();
      doc.put("_id", new ObjectId(sb.toString()));
      List<DBObject> docList = tableDocumentList.get(table);
      docList.add(doc);
      if (tupleId % batchSize == 0) { // do batch insert here
        db.getCollection(table).insert(docList);
        tableDocumentList.put(table, new ArrayList<DBObject>());
      }
      else {
        tableDocumentList.put(table, docList);
      }
    }
    ++tupleId;
  }
}
