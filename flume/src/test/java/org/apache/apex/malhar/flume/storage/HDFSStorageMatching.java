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

import com.google.common.primitives.Ints;

import com.datatorrent.netlet.util.Slice;

/**
 *
 */
public class HDFSStorageMatching
{

  public static void main(String[] args)
  {
    HDFSStorage storage = new HDFSStorage();
    storage.setBaseDir(args[0]);
    storage.setId(args[1]);
    storage.setRestore(true);
    storage.setup(null);
    int count = 100000000;

    logger.debug(" start time {}", System.currentTimeMillis());
    int index = 10000;
    byte[] b = Ints.toByteArray(index);
    for (int i = 0; i < count; i++) {
      storage.store(new Slice(b, 0, b.length));
      index++;
      b = Ints.toByteArray(index);
    }
    storage.flush();
    for (int i = 0; i < count; i++) {
      storage.store(new Slice(b, 0, b.length));
      index++;
      b = Ints.toByteArray(index);
    }
    storage.flush();
    for (int i = 0; i < count; i++) {
      storage.store(new Slice(b, 0, b.length));
      index++;
      b = Ints.toByteArray(index);
    }
    storage.flush();
    for (int i = 0; i < count; i++) {
      storage.store(new Slice(b, 0, b.length));
      index++;
      b = Ints.toByteArray(index);
    }
    storage.flush();
    for (int i = 0; i < count; i++) {
      storage.store(new Slice(b, 0, b.length));
      index++;
      b = Ints.toByteArray(index);
    }
    storage.flush();
    logger.debug(" end time {}", System.currentTimeMillis());
    logger.debug(" start time for retrieve {}", System.currentTimeMillis());
    b = storage.retrieve(new byte[8]);
    int org_index = index;
    index = 10000;
    match(b, index);
    while (true) {
      index++;
      b = storage.retrieveNext();
      if (b == null) {
        logger.debug(" end time for retrieve {}/{}/{}", System.currentTimeMillis(), index, org_index);
        return;
      } else {
        if (!match(b, index)) {
          throw new RuntimeException("failed : " + index);
        }
      }
    }

  }

  public static boolean match(byte[] data, int match)
  {
    byte[] tempData = new byte[data.length - 8];
    System.arraycopy(data, 8, tempData, 0, tempData.length);
    int dataR = Ints.fromByteArray(tempData);
    //logger.debug("input: {}, output: {}",match,dataR);
    if (match == dataR) {
      return true;
    }
    return false;
  }

  private static final Logger logger = LoggerFactory.getLogger(HDFSStorageMatching.class);
}

