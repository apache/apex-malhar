/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.dedup;

import java.util.Map;

import javax.annotation.Nonnull;

import com.google.common.collect.Maps;

import com.datatorrent.api.DAG;

import com.datatorrent.lib.bucket.Bucketable;
import com.datatorrent.lib.bucket.Context;
import com.datatorrent.lib.bucket.HdfsBucketStore;

/**
 * {@link Deduper} that uses hdfs to store buckets.
 */
public abstract class DeduperWithHdfsStore<INPUT extends Bucketable, OUTPUT> extends Deduper<INPUT, OUTPUT>
{
  @Nonnull
  private String bucketsPath;

  public DeduperWithHdfsStore()
  {
    super();
    bucketsPath = "buckets";
  }

  public void setBucketsPath(@Nonnull String bucketsPath)
  {
    this.bucketsPath = bucketsPath;
  }

  @Override
  protected Context getBucketContext(com.datatorrent.api.Context.OperatorContext context)
  {
    Map<String, Object> parameters = Maps.newHashMap();
    parameters.put(HdfsBucketStore.STORE_ROOT, context.getValue(DAG.APPLICATION_PATH) + "/" + bucketsPath);
    parameters.put(HdfsBucketStore.OPERATOR_ID, context.getId());
    parameters.put(HdfsBucketStore.PARTITION_KEYS, partitionKeys);
    parameters.put(HdfsBucketStore.PARTITION_MASK, partitionMask);

    return new com.datatorrent.lib.bucket.Context(parameters);
  }

  @Override
  public void partitioned(Map<Integer, Partition<Deduper<INPUT, OUTPUT>>> partitions)
  {
    super.partitioned(partitions);
    for (Partition<Deduper<INPUT, OUTPUT>> partition : partitions.values()) {
      ((DeduperWithHdfsStore<INPUT, OUTPUT>) partition.getPartitionedInstance()).bucketsPath = this.bucketsPath;
    }
  }
}
