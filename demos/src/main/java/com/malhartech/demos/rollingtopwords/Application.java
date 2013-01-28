/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.rollingtopwords;

import com.malhartech.api.DAG;
import com.malhartech.api.ApplicationFactory;
import org.apache.hadoop.conf.Configuration;

/**
 * This program will output the top N frequent word from twitter feed
 * 
 * @author Zhongjian Wang<zhongjian@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{
  @Override
  public DAG getApplication(Configuration conf)
  {
    return new TwitterTopWordCounter(conf);
  }
}
