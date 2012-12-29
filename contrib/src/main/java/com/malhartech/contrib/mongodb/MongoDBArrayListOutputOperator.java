/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.mongodb;

import com.mongodb.BasicDBObject;
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MongoDBArrayListOutputOperator extends MongoDBOutputOperator<ArrayList<Object>>
{
  private static final Logger logger = LoggerFactory.getLogger(MongoDBArrayListOutputOperator.class);

  @Override
  public void processTuple(ArrayList<Object> tuple)
  {
    BasicDBObject doc = new BasicDBObject();

//    doc.put(applicationIdName, 0);
    doc.put(operatorIdName, operatorId);
    doc.put(windowIdName, windowId);
//    db.getCollection(table).insert(doc);

    if (windowId > lastWindowId) {
      lastWindowId = windowId;
      BasicDBObject where = new BasicDBObject();
//      doc1.put(applicationIdName, 0);
      where.put(operatorIdName, operatorId);
      BasicDBObject value = new BasicDBObject();
//      doc2.put(applicationIdName, 0);
      value.put(operatorIdName, operatorId);
      value.put(windowIdName, windowId);
      maxWindowCollection.update(where, value);

    }
  }

}
