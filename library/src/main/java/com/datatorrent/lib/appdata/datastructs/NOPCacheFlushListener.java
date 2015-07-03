/*
 * Copyright (c) 2015 DataTorrent, Inc.
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
package com.datatorrent.lib.appdata.datastructs;

import com.datatorrent.lib.appdata.datastructs.CacheLRUSynchronousFlush.CacheFlushListener;

/**
 * This is a listener which is called by {@link CacheLRUSynchronousFlush} when the cache is flushed.
 * @param <KEY> The type of keys in the cache.
 * @param <VALUE> The type of values in the cache.
 */
public class NOPCacheFlushListener<KEY, VALUE> implements CacheFlushListener<KEY, VALUE>
{
  @Override
  public void flush(KEY key, VALUE value)
  {
    //Do nothing
  }
}
