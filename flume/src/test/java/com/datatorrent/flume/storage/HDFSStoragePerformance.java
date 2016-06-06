/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.flume.storage;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.netlet.util.Slice;

/**
 * @author Gaurav Gupta  <gaurav@datatorrent.com>
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

