/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.mongodb;

import com.mongodb.*;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MongoDBTest
{
  private static char[] password;
  private static String userName;

  void test() throws UnknownHostException
  {
    Mongo m = new Mongo();
//      Mongo m1 = new Mongo("localhost");
//      Mongo m2 = new Mongo("localhost",27017);
//      Mongo m3 = new Mongo(Arrays.asList(new ServerAddress("localhost",27017),
//                                         new ServerAddress("localhost",27018),
//                                         new ServerAddress("localhost",27019)));
    DB db = m.getDB("test");
//      boolean auth = db.authenticate(userName, password);
    Set<String> colls = db.getCollectionNames();
    System.out.println("output:");
    for (String s : colls) {
      System.out.println(s);
    }

    DBCollection coll = db.getCollection("testCollection");
/*
     BasicDBObject doc = new BasicDBObject();
     doc.put("name", "MongoDB");
     doc.put("type", "database");
     doc.put("count", 1);
     BasicDBObject info = new BasicDBObject();
     info.put("x", 203);
     info.put("y", 102);
     doc.put("info", info);
     coll.insert(doc);
     m.setWriteConcern(WriteConcern.SAFE);

    DBObject myDoc = coll.findOne();
    System.out.println(myDoc);
    for (int i = 0; i < 100; i++) {
      coll.insert(new BasicDBObject().append("i", i));
    }
    * */
    System.out.println(coll.getCount());
        DBCursor cursor = coll.find();
//        try {
//            while(cursor.hasNext()) {
//                System.out.println(cursor.next());
//            }
//        } finally {
//            cursor.close();
//        }
        BasicDBObject query = new BasicDBObject();

        query.put("j", new BasicDBObject("$ne", 3));
        query.put("k", new BasicDBObject("$gt", 10));

        cursor = coll.find(query);

        try {
            while(cursor.hasNext()) {
                System.out.println(cursor.next());
            }
        } finally {
            cursor.close();
        }
        coll.createIndex(new BasicDBObject("i", 1));
        List<DBObject> list = coll.getIndexInfo();
        for(DBObject o: list) {
          System.out.println(o);
        }
  }

  public static void main(String[] args) throws Exception
  {
    MongoDBTest qt = new MongoDBTest();
    qt.test();

  }
}
