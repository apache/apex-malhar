/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.primitives.Ints;

import com.datatorrent.netlet.util.Slice;

/**
 * <p>HDFSStoragePerformanceTest class.</p>
 *
 * @author Gaurav Gupta  <gaurav@datatorrent.com>
 * @since 1.0.1
 */
public class HDFSStoragePerformanceTest
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

  private static final Logger logger = LoggerFactory.getLogger(HDFSStoragePerformanceTest.class);
}

