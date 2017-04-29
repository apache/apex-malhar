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
package org.apache.apex.malhar.sql.sample;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Arrays;
import java.util.Collection;
import java.util.TimeZone;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.google.common.base.Predicates;
import com.google.common.collect.Collections2;

import com.datatorrent.api.LocalMode;


public class SQLApplicationWithAPITest
{
  private TimeZone defaultTZ;

  @Before
  public void setUp() throws Exception
  {
    defaultTZ = TimeZone.getDefault();
    TimeZone.setDefault(TimeZone.getTimeZone("GMT"));
  }

  @After
  public void tearDown() throws Exception
  {
    TimeZone.setDefault(defaultTZ);
  }

  @Test
  public void test() throws Exception
  {
    LocalMode lma = LocalMode.newInstance();
    Configuration conf = new Configuration(false);
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties.xml"));
    conf.addResource(this.getClass().getResourceAsStream("/META-INF/properties-SQLApplicationWithAPI.xml"));

    SQLApplicationWithAPI app = new SQLApplicationWithAPI();

    lma.prepareDAG(app, conf);

    LocalMode.Controller lc = lma.getController();

    PrintStream originalSysout = System.out;
    final ByteArrayOutputStream baos = new ByteArrayOutputStream();
    System.setOut(new PrintStream(baos));

    lc.runAsync();
    SQLApplicationWithModelFileTest.waitTillStdoutIsPopulated(baos, 30000);
    lc.shutdown();

    System.setOut(originalSysout);

    String[] sout = baos.toString().split(System.lineSeparator());
    Collection<String> filter = Collections2.filter(Arrays.asList(sout), Predicates.containsPattern("Delta Record:"));

    String[] actualLines = filter.toArray(new String[filter.size()]);
    Assert.assertTrue(actualLines[0].contains("RowTime=Mon Feb 15 10:15:00 GMT 2016, Product=paint1"));
    Assert.assertTrue(actualLines[1].contains("RowTime=Mon Feb 15 10:16:00 GMT 2016, Product=paint2"));
    Assert.assertTrue(actualLines[2].contains("RowTime=Mon Feb 15 10:17:00 GMT 2016, Product=paint3"));
    Assert.assertTrue(actualLines[3].contains("RowTime=Mon Feb 15 10:18:00 GMT 2016, Product=paint4"));
    Assert.assertTrue(actualLines[4].contains("RowTime=Mon Feb 15 10:19:00 GMT 2016, Product=paint5"));
    Assert.assertTrue(actualLines[5].contains("RowTime=Mon Feb 15 10:10:00 GMT 2016, Product=abcde6"));
  }
}
