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
package com.datatorrent.lib.bucket;

import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ExpirableHdfsBucketStore<T extends Bucketable & Event> extends HdfsBucketStore<T> implements BucketStore.ExpirableBucketStore<T>
{

  @Override
  public void deleteExpiredBuckets(long time) throws IOException
  {
    Iterator<Long> iterator = windowToBuckets.keySet().iterator();
    for (; iterator.hasNext(); ) {
      long window = iterator.next();
      long timestamp= windowToTimestamp.get(window);
      if (timestamp < time) {
        Collection<Integer> indices = windowToBuckets.get(window);
        synchronized (indices) {
          if (indices.size() > 0) {
            Path dataFilePath = new Path(bucketRoot + PATH_SEPARATOR + window);
            FileSystem fs = FileSystem.newInstance(dataFilePath.toUri(), configuration);
            try {
              if (fs.exists(dataFilePath)) {
                logger.debug("start delete {}", window);
                fs.delete(dataFilePath, true);
                logger.debug("end delete {}", window);
              }
              for (int bucketIdx : indices) {
                Map<Long, Long> offsetMap = bucketPositions[bucketIdx];
                if (offsetMap != null) {
                  synchronized (offsetMap) {
                    offsetMap.remove(window);
                  }
                }
              }
            }
            finally {
              fs.close();
            }
          }
          windowToTimestamp.remove(window);
          iterator.remove();
        }
      }
    }
  }

  private static transient final Logger logger = LoggerFactory.getLogger(ExpirableHdfsBucketStore.class);
}
