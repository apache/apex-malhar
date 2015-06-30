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
package com.datatorrent.lib.appdata.schemas;

import com.google.common.collect.Sets;
import org.junit.Assert;
import org.junit.Test;

public class DataQuerySnapshotTest
{
  @Test
  public void simpleDataQuerySnapshotOneTime()
  {
    Fields fields = new Fields(Sets.newHashSet("a", "b"));

    DataQuerySnapshot query = new DataQuerySnapshot("1",
                                                  fields);

    Assert.assertEquals("This query should be oneTime.", true, query.isOneTime());
  }

  @Test
  public void simpeDataQuerySnapshotCountdown()
  {
    Fields fields = new Fields(Sets.newHashSet("a", "b"));

    DataQuerySnapshot query = new DataQuerySnapshot("1",
                                                  fields,
                                                  1L);

    Assert.assertEquals("This query should be oneTime.", false, query.isOneTime());
  }
}
