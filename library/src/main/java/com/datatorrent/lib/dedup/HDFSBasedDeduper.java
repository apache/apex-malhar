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
package com.datatorrent.lib.dedup;

import com.datatorrent.api.Context;
import com.datatorrent.api.DAG;

import com.datatorrent.lib.bucket.BucketStore;
import com.datatorrent.lib.bucket.HdfsBucketStore;
import com.datatorrent.lib.bucket.TimeEvent;

/**
 * A deduper which uses HDFS as its backing store.
 */
public abstract class HDFSBasedDeduper<INPUT extends TimeEvent, OUTPUT> extends DeduperWithTimeBuckets<INPUT, OUTPUT>
{
  @Override
  protected BucketStore<INPUT> getBucketStore(Context.OperatorContext context)
  {
    return new HdfsBucketStore<INPUT>(context.getValue(DAG.APPLICATION_PATH), context.getId(), maxNoOfBucketsInDir,
      partitionKeys, partitionMask);
  }

}
