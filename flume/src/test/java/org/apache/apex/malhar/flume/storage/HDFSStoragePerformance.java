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
package org.apache.apex.malhar.flume.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.util.Slice;

/**
 *
 */
public class HDFSStoragePerformance
{

  public static void main(String[] args)
  {
    HDFSStorage storage = new HDFSStorage();
    storage.setBaseDir(".");
    storage.setId("gaurav_flume_1");
    storage.setRestore(true);
    storage.setup(null);
    int count = 1000000;

    logger.debug(" start time {}", System.currentTimeMillis());
    int index = 10000;
    byte[] b = new byte[1024];
    for (int i = 0; i < count; i++) {
      storage.store(new Slice(b, 0, b.length));
    }
    storage.flush();
    for (int i = 0; i < count; i++) {
      storage.store(new Slice(b, 0, b.length));
    }
    storage.flush();
    for (int i = 0; i < count; i++) {
      storage.store(new Slice(b, 0, b.length));
    }
    storage.flush();
    logger.debug(" end time {}", System.currentTimeMillis());
    logger.debug(" start time for retrieve {}", System.currentTimeMillis());
    storage.retrieve(new byte[8]);
    String inputData = new String(b);
    index = 1;
    while (true) {
      b = storage.retrieveNext();
      if (b == null) {
        logger.debug(" end time for retrieve {}", System.currentTimeMillis());
        return;
      } else {
        if (!match(b, inputData)) {
          throw new RuntimeException("failed : " + index);
        }
      }

      index++;
    }

  }

  public static boolean match(byte[] data, String match)
  {
    byte[] tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
//    logger.debug("input: {}, output: {}",match,new String(tempData));
    return (match.equals(new String(tempData)));
  }

  private static final Logger logger = LoggerFactory.getLogger(HDFSStoragePerformance.class);
}

