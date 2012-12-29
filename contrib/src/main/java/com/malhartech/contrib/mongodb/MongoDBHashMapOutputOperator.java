/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.mongodb;

import com.mongodb.BasicDBObject;
import java.nio.ByteBuffer;
import java.util.HashMap;
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
    BasicDBObject doc = new BasicDBObject();
    String table = null;
    for (Map.Entry<String, T> entry : tuple.entrySet()) {
      doc.put(entry.getKey(), entry.getValue());
      if (table == null) {
        table = propTable.get(entry.getKey());
      }
    }
//    doc.put(applicationIdName, 0);

    ByteBuffer bb = ByteBuffer.allocate(12);
    bb.putLong(windowId);
    bb.putInt(tupleId);
//    String str = Hex.encodeHexString(bb.array());
    StringBuilder sb = new StringBuilder();
    for( byte b: bb.array() ) {
      sb.append(String.format("%02x", b&0xff));
    }
    doc.put("_id", new ObjectId(sb.toString()));
    doc.put(operatorIdName, operatorId);
//    doc.put(windowIdName, windowId);
    db.getCollection(table).insert(doc);

    if (windowId > lastWindowId) {
      lastWindowId = windowId;
      BasicDBObject doc1 = new BasicDBObject();
//      doc1.put(applicationIdName, 0);
      doc1.put(operatorIdName, operatorId);
      BasicDBObject doc2 = new BasicDBObject();
//      doc2.put(applicationIdName, 0);
      doc2.put(operatorIdName, operatorId);
      doc2.put(windowIdName, windowId);
      maxWindowCollection.update(doc1, doc2);

    }
    tupleId++;
  }
}
