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
package org.apache.apex.examples.twitter;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import static org.junit.Assert.assertEquals;

import com.datatorrent.api.DAG;
import com.datatorrent.api.LocalMode;

/**
 * Test for the application which taps into the twitter's sample input stream and
 * dumps all the tweets into  a database.
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
