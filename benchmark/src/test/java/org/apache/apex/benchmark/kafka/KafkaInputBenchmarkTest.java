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
package org.apache.apex.benchmark.kafka;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;

import org.junit.Test;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.LocalMode;

public class KafkaInputBenchmarkTest
{
  @Test
  public void testBenchmark() throws FileNotFoundException
  {
    Configuration conf = new Configuration();
    InputStream is = new FileInputStream("src/site/conf/dt-site-kafka.xml");
    conf.addResource(is);

    LocalMode lma = LocalMode.newInstance();

    try {
      lma.prepareDAG(new KafkaInputBenchmark(), conf);
      LocalMode.Controller lc = lma.getController();
      lc.run(30000);
    } catch (Exception ex) {
      throw new RuntimeException(ex);
    }
  }
}
