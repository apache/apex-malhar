/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.mongodb;

import com.mongodb.BasicDBObject;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
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
    HashMap<String, BasicDBObject> tableDoc = new HashMap<String, BasicDBObject>();
    for (Map.Entry<String, T> entry : tuple.entrySet()) {
      BasicDBObject doc;
      String table = propTable.get(entry.getKey());
      if ((doc = tableDoc.get(table)) == null) {
        doc = new BasicDBObject();
        doc.put(entry.getKey(), entry.getValue());
      }
      else {
        doc.put(entry.getKey(), entry.getValue());
      }
      tableDoc.put(table, doc);
    }
//    doc.put(applicationIdName, 0);

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
      BasicDBObject doc = entry.getValue();
      doc.put("_id", new ObjectId(sb.toString()));
      db.getCollection(entry.getKey()).insert(doc);
    }
//    doc.put("_id", new ObjectId(sb.toString()));
//    doc.put(operatorIdName, operatorId);
//    doc.put(windowIdName, windowId);

//    db.getCollection(table).insert(doc);

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
//    System.out.println("sec:" + baseSec + " winId:" + winId + " tupleId:" + tupleId);
  }
}
