/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.rollingtopwords;

import com.malhartech.api.DAG;
import com.malhartech.api.ApplicationFactory;
import org.apache.hadoop.conf.Configuration;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
public class Application implements ApplicationFactory
{
  @Override
  public DAG getApplication(Configuration conf)
  {
    return new TwitterTopCounter(conf);
  }
}
