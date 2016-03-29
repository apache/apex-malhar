/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */

package com.datatorrent.contrib.enrichment;

import com.datatorrent.lib.db.cache.CacheManager;

/**
 * @since 3.1.0
 */
public class NullValuesCacheManager extends CacheManager
{

  private static final NullObject NULL = new NullObject();
  @Override
  public Object get(Object key)
  {
    Object primaryVal = primary.get(key);
    if (primaryVal != null) {
      if (primaryVal == NULL) {
        return null;
      }

      return primaryVal;
    }

    Object backupVal = backup.get(key);
    if (backupVal != null) {
      primary.put(key, backupVal);
    } else {
      primary.put(key, NULL);
    }
    return backupVal;

  }

  private static class NullObject
  {
  }
}

