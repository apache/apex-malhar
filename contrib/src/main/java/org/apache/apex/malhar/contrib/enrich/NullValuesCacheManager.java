/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.enrich;

import org.apache.apex.malhar.lib.db.cache.CacheManager;
import org.apache.hadoop.classification.InterfaceStability;

/**
 * Null Values Cache Manager. Using this NULL entries can be specified explicitly.
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
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

