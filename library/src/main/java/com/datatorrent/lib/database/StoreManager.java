/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.database;

import java.util.Map;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import com.google.common.base.Preconditions;

/**
 * <br>Manages primary and secondary stores.</br>
 * <br>It firsts checks the primary store for a key. If the primary store doesn't have the key, it queries the backup store and retrieves the value.</br>
 * <br>If the key was present in the backup store, its value is returned and also saves the key/value in the primary store.</br>
 * <br>Typically primary store is faster but has limited size like memory and backup store is slower but unlimited like databases.</br>
 * <br>This is non-threadsafe.</br>
 */
public class StoreManager
{
  protected final Store.Primary primary;
  protected final Store.Backup backup;

  public StoreManager(Store.Primary primary, Store.Backup backup)
  {
    this.primary = Preconditions.checkNotNull(primary, "primary store");
    this.backup = Preconditions.checkNotNull(backup, "backup store");
  }

  public void initialize()
  {
    Map<Object, Object> initialEntries = backup.fetchStartupData();
    if(initialEntries!=null){
      primary.bulkSet(initialEntries);
    }
  }

  public void shutdown()
  {
    primary.shutdownStore();
    backup.shutdownStore();
  }

  @Nullable
  public Object get(@Nonnull Object key)
  {
    synchronized (key) {
      Object primaryVal = primary.getValueFor(key);
      if (primaryVal != null) {
        return primaryVal;
      }

      Object backupVal = backup.getValueFor(key);
      if (backupVal != null) {
        primary.setValueFor(key, backupVal);
        return backupVal;
      }
      return null;
    }
  }
}
