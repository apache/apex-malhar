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
package org.apache.apex.malhar.lib.state.managed;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import javax.annotation.Nullable;

import org.junit.Assert;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.fileaccess.FileAccess;
import org.apache.apex.malhar.lib.util.comparator.SliceComparator;
import org.apache.apex.malhar.lib.utils.serde.BufferSlice;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.RemoteIterator;

import com.google.common.base.Function;
import com.google.common.collect.Maps;

import com.datatorrent.api.Attribute;
import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.netlet.util.Slice;

import static org.apache.apex.malhar.lib.helper.OperatorContextTestHelper.mockOperatorContext;

public class ManagedStateTestUtils
{
  /**
   * Validates the bucket data on the File System.
   * @param fileAccess        file access
   * @param bucketId          bucket id
   * @param unsavedBucket     bucket data to compare with.
   * @param keysPerTimeBucket num keys per time bucket
   * @throws IOException
   */
  public static void validateBucketOnFileSystem(FileAccess fileAccess, long bucketId,
      Map<Slice, Bucket.BucketedValue> unsavedBucket, int keysPerTimeBucket) throws IOException
  {
    RemoteIterator<LocatedFileStatus> iterator = fileAccess.listFiles(bucketId);
    TreeMap<Slice, Slice> fromDisk = Maps.newTreeMap(new SliceComparator());
    int size = 0;
    while (iterator.hasNext()) {
      LocatedFileStatus fileStatus = iterator.next();

      String timeBucketStr = fileStatus.getPath().getName();
      if (timeBucketStr.equals(BucketsFileSystem.META_FILE_NAME) || timeBucketStr.endsWith(".tmp")) {
        //ignoring meta file
        continue;
      }

      LOG.debug("bucket {} time-bucket {}", bucketId, timeBucketStr);

      FileAccess.FileReader reader = fileAccess.getReader(bucketId, timeBucketStr);

      reader.readFully(fromDisk);
      size += keysPerTimeBucket;
      Assert.assertEquals("size of bucket " + bucketId, size, fromDisk.size());
    }

    Assert.assertEquals("size of bucket " + bucketId, unsavedBucket.size(), fromDisk.size());

    Map<Slice, Slice> testBucket = Maps.transformValues(unsavedBucket, new Function<Bucket.BucketedValue, Slice>()
    {
      @Override
      public Slice apply(@Nullable Bucket.BucketedValue input)
      {
        assert input != null;
        return input.getValue();
      }
    });
    Assert.assertEquals("data of bucket" + bucketId, testBucket, fromDisk);
  }

  public static Map<Long, Map<Slice, Bucket.BucketedValue>> getTestData(int startBucket, int endBucket, int keyStart)
  {
    Map<Long, Map<Slice, Bucket.BucketedValue>> data = Maps.newHashMap();
    for (int i = startBucket; i < endBucket; i++) {
      Map<Slice, Bucket.BucketedValue> bucketData = getTestBucketData(keyStart, 100);
      data.put((long)i, bucketData);
    }
    return data;
  }

  public static Map<Slice, Bucket.BucketedValue> getTestBucketData(int keyStart, long timeBucketStart)
  {
    Map<Slice, Bucket.BucketedValue> bucketData = Maps.newHashMap();
    for (int j = 0; j < 5; j++) {
      Slice keyVal = new Slice(Integer.toString(keyStart).getBytes());
      bucketData.put(keyVal, new Bucket.BucketedValue(timeBucketStart + j, keyVal));
      keyStart++;
    }
    return bucketData;
  }

  public static OperatorContext getOperatorContext(int operatorId, String applicationPath)
  {
    Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    attributes.put(DAG.APPLICATION_PATH, applicationPath);
    return mockOperatorContext(operatorId, attributes);
  }

  public static OperatorContext getOperatorContext(int operatorId)
  {
    Attribute.AttributeMap.DefaultAttributeMap attributes = new Attribute.AttributeMap.DefaultAttributeMap();
    return mockOperatorContext(operatorId, attributes);
  }

  private static final transient Logger LOG = LoggerFactory.getLogger(ManagedStateTestUtils.class);

  public static Slice getSliceFor(String x)
  {
    return new BufferSlice(x.getBytes());
  }
}
