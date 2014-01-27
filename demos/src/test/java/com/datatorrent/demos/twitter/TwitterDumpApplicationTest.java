/*
 *  Copyright (c) 2012-2014 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.datatorrent.demos.twitter;

import org.junit.Test;
import static org.junit.Assert.*;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

/**
 *
 * @author Chetan Narsude <chetan@datatorrent.com>
 */
public class TwitterDumpApplicationTest
{
  @Test
  public void testPopulateDAG() throws Exception
  {
    Configuration configuration = new Configuration(false);

    LocalMode lm = LocalMode.newInstance();
    DAG prepareDAG = lm.prepareDAG(new TwitterDumpApplication(), configuration);
    DAG clonedDAG = lm.cloneDAG();

    assertEquals("Serialization", prepareDAG, clonedDAG);
  }

}