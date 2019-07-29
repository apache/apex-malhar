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
package org.apache.apex.malhar.solace;

import java.io.IOException;
import java.util.Arrays;
import java.util.Set;

import org.apache.hadoop.fs.FileStatus;

import com.google.common.collect.Sets;
import com.datatorrent.api.Context;
import com.datatorrent.lib.io.IdempotentStorageManager;

/**
 *
 */
public class FSOpsIdempotentStorageManager extends IdempotentStorageManager.FSIdempotentStorageManager
{

  protected transient Context.OperatorContext context;

  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    this.context = context;
  }

  public FSOpsIdempotentStorageManager getIdempotentStateSnapshot()
  {
    FSOpsIdempotentStorageManager snapshot = new FSOpsIdempotentStorageManager();
    snapshot.setup(context);
    return snapshot;
  }

  public Set<Integer> getOperatorIds() throws IOException
  {
    Set<Integer> ids = Sets.newLinkedHashSet();
    FileStatus[] fileStatuses = fs.listStatus(appPath);
    for (FileStatus fileStatus : fileStatuses) {
      ids.add(Integer.parseInt(fileStatus.getPath().getName()));
    }
    return ids;
  }

  public long[] getOrderedWindowIds(int operatorId) throws IOException
  {
    long[] windowIds = getWindowIds(operatorId);
    Arrays.sort(windowIds);
    return windowIds;
  }

}
