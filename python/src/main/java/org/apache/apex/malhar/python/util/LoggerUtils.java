/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.python.util;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.python.operator.PythonGenericOperator;

public class LoggerUtils
{

  public static class InputStreamConsumer extends Thread
  {
    private static final Logger LOG = LoggerFactory.getLogger(PythonGenericOperator.class);
    private InputStream is;
    private String name;
    private StreamType streamType;
    private String processId;

    public enum StreamType
    {
      ERROR, OUTPUT
    }

    public InputStreamConsumer(String name, InputStream is, StreamType streamType)
    {
      this.is = is;
      this.name = name;
      this.streamType = streamType;
    }

    @Override
    public void run()
    {
      LOG.info("Starting Stream Gobbler " + this.name);
      try {

        InputStreamReader isr = new InputStreamReader(this.is);
        BufferedReader br = new BufferedReader(isr);
        String line;
        while ((line = br.readLine()) != null) {
          if (this.streamType == StreamType.ERROR) {
            LOG.error(" From other process :" + line);
          } else {
            LOG.info(" From other process :" + line);

          }
        }
      } catch (IOException exp) {
        exp.printStackTrace();
      }

      LOG.info("Exiting Stream Gobbler " + this.name);
    }
  }

  public static void captureProcessStreams(Process process)
  {
    InputStreamConsumer stdoutConsumer = new InputStreamConsumer("outputStream", process.getInputStream(), InputStreamConsumer.StreamType.OUTPUT);
    InputStreamConsumer erroConsumer = new InputStreamConsumer("errorStream", process.getErrorStream(), InputStreamConsumer.StreamType.ERROR);
    erroConsumer.start();
    stdoutConsumer.start();
  }
}
