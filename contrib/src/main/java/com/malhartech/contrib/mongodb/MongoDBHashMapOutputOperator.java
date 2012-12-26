/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.mongodb;

import com.mongodb.BasicDBObject;
import java.util.HashMap;
import java.util.Map;

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
    for (Map.Entry<String, T> entry: tuple.entrySet()) {
      doc.put(entry.getKey(), entry.getValue());
      if( table == null ) {
        table = propTable.get(entry.getKey());
      }
    }
    doc.put(windowIdName, windowId);
    doc.put(applicationIdName, 0);
    doc.put(operatorIdName, operatorId);
    db.getCollection(table).insert(doc);
  }
}
