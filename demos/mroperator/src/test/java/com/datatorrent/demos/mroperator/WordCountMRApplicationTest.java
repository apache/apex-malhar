/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
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
package com.datatorrent.demos.mroperator;

import java.io.File;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.FileUtils;
import org.apache.hadoop.conf.Configuration;
import org.junit.Assert;
import org.junit.Rule;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Maps;

import com.datatorrent.api.LocalMode;

public class WordCountMRApplicationTest
{
  private static Logger LOG = LoggerFactory.getLogger(WordCountMRApplicationTest.class);
  @Rule
  public MapOperatorTest.TestMeta testMeta = new MapOperatorTest.TestMeta();

  @Test
  public void testSomeMethod() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.set("dt.application.WordCountDemo.operator.Mapper.dirName", testMeta.testDir);
    conf.setInt("dt.application.WordCountDemo.operator.Mapper.partitionCount", 1);
    conf.set("dt.application.WordCountDemo.operator.Console.filePath", testMeta.testDir);
    conf.set("dt.application.WordCountDemo.operator.Console.outputFileName", "output.txt");
    lma.prepareDAG(new NewWordCountApplication(), conf);
    LocalMode.Controller lc = lma.getController();
    lc.setHeartbeatMonitoringEnabled(false);
    lc.run(5000);
    lc.shutdown();
    List<String> readLines = FileUtils.readLines(new File(testMeta.testDir + "/output.txt"));
    Map<String,Integer> readMap = Maps.newHashMap();
    Iterator<String> itr = readLines.iterator();
    while(itr.hasNext()){
      String[] splits = itr.next().split("=");
      readMap.put(splits[0],Integer.valueOf(splits[1]));
    }
    Map<String,Integer> expectedMap = Maps.newHashMap();
    expectedMap.put("1",2);
    expectedMap.put("2",2);
    expectedMap.put("3",2);
    Assert.assertEquals("expected reduced data ", expectedMap, readMap);
    LOG.info("read lines {}", readLines);
  }

}
