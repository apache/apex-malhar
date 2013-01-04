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

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MongoDBHashMapOutputOperator<T> extends MongoDBOutputOperator<HashMap<String, T>>
{
  @Override
  public void processTuple(HashMap<String, T> tuple)
  {
    if (windowId > lastWindowId) {
      lastWindowId = windowId;
      BasicDBObject doc1 = new BasicDBObject();
//      doc1.put(applicationIdName, 0);
      doc1.put(operatorIdColumnName, operatorId);
      BasicDBObject doc2 = new BasicDBObject();
//      doc2.put(applicationIdName, 0);
      doc2.put(operatorIdColumnName, operatorId);
      doc2.put(windowIdColumnName, windowId);
      maxWindowCollection.update(doc1, doc2);
    }

    HashMap<String, BasicDBObject> tableDoc = new HashMap<String, BasicDBObject>();
      BasicDBObject doc=null;
    for (Map.Entry<String, T> entry : tuple.entrySet()) {
      String table = propTableMap.get(entry.getKey());
      if ((doc = tableDoc.get(table)) == null) {
        doc = new BasicDBObject();
        doc.put(entry.getKey(), entry.getValue());
      }
      else {
        doc.put(entry.getKey(), entry.getValue());
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
