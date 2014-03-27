/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.contrib.mongodb;

import java.util.*;

import javax.validation.constraints.NotNull;

import com.mongodb.BasicDBObject;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.db.DataStoreWriter;

/**
 * Does batch inserts of map objects into mongoDB
 *
 * @param <K> map key
 * @param <V> map value
 */
public class MongoDBMapWriter<K, V> extends AbstractMongoDBConnectable implements DataStoreWriter<Map<K, V>>
{
  /*
   * table in which data is inserted
   */
  @NotNull
  protected String table;
  /*
   * windowId used to track non duplicate batch inserts
   */
  protected long lastInsertedWindowId;

  @Override
  public void batchInsert(List<Map<K, V>> tupleList, long windowId)
  {
    if (windowId > lastInsertedWindowId) {
      ArrayList<DBObject> docList = new ArrayList<DBObject>();
      for (Map<K, V> tuple : tupleList) {
        BasicDBObject doc = new BasicDBObject();
        doc.putAll(tuple);
        docList.add(doc);
      }
      db.getCollection(table).insert(docList);
      lastInsertedWindowId = windowId;
    }
    else {
      logger.debug("Already inserted records for window id {}", windowId);
    }
  }

  /**
   * Returns list of all tuples from the specified table
   *
   * @param table table to query from
   * @return
   */
  @SuppressWarnings("rawtypes")
  public List<Map> find(String table)
  {
    DBCursor cursor = db.getCollection(table).find();
    DBObject next;
    ArrayList<Map> result = new ArrayList<Map>();
    while (cursor.hasNext()) {
      next = cursor.next();
      result.add(next.toMap());
    }
    return result;
  }

  /**
   * Drops the specified table from the database
   *
   * @param table table to drop
   */
  public void dropTable(String table)
  {
    db.getCollection(table).drop();
  }

  /**
   * setter for table to write to
   *
   * @param table table
   */
  public void setTable(String table)
  {
    this.table = table;
  }

  private static final Logger logger = LoggerFactory.getLogger(MongoDBMapWriter.class);

  @Override
  public void batchUpsert(List<Map<K, V>> tupleList, List<String> upsertKey, long windowId)
  {
    //TODO: implement
  }
}
