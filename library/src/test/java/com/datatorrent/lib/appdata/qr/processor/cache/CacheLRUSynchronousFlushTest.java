/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.appdata.qr.processor.cache;

import org.junit.Assert;
import org.junit.Test;

public class CacheLRUSynchronousFlushTest
{
  @Test
  public void simplePutAndRemove()
  {
    final Integer key = 1;
    final Integer value = 1;

    CacheLRUSynchronousFlush<Integer, Integer> cache =
    new CacheLRUSynchronousFlush<Integer, Integer>(new NopCacheFlushListener<Integer, Integer>());

    cache.put(key, value);

    Assert.assertEquals("The values should equal", value, cache.get(key));

    Integer removedVal = cache.remove(key);

    Assert.assertEquals("The removed value must equal the old value", value, removedVal);
    Assert.assertEquals("The retrieved value should be null.", null, cache.get(key));
  }
}
