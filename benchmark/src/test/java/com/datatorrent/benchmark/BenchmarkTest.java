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
package com.datatorrent.benchmark;

import java.io.IOException;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DAG.Locality;
import com.datatorrent.api.LocalMode;
import com.datatorrent.benchmark.Benchmark;

/**
 * Test the DAG declaration in local mode.
 */
public class BenchmarkTest
{
  @Test
  public void testApplication() throws IOException, Exception
  {
    for (final Locality l : Locality.values()) {
      logger.debug("Running the with {} locality", l);
      LocalMode.runApp(new Benchmark.AbstractApplication ()
      {
        @Override
        public Locality getLocality()
        {
          return l;
        }

      }, 60000);
    }
  }

  private static final Logger logger = LoggerFactory.getLogger(BenchmarkTest.class);
}
