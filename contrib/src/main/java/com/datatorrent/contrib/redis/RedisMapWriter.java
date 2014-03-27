/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.contrib.redis;

import com.datatorrent.lib.db.TransactionalDataStoreWriter;
import java.util.List;
import java.util.Map;

/**
 *
 * @author Ashwin Chandra Putta <ashwin@datatorrent.com>
 */
public class RedisMapWriter extends RedisStore implements TransactionalDataStoreWriter<Map<Object, Object>>
{
  @Override
  public void batchInsert(List<Map<Object, Object>> tupleList, long windowId)
  {
    for (Map<Object, Object> map : tupleList) {
      putAll(map);
    }
  }

  @Override
  public void batchUpsert(List<Map<Object, Object>> tupleList, List<String> upsertKey, long windowId)
  {
    // TODO: implement
  }
}
