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
package com.example;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import javax.validation.constraints.NotNull;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.StreamCodec;
import com.datatorrent.api.annotation.InputPortFieldAnnotation;
import com.datatorrent.lib.io.fs.AbstractSingleFileOutputOperator;

public class ValidationToFile extends AbstractSingleFileOutputOperator<byte[]>
{
  private static final Logger LOG = LoggerFactory.getLogger(ValidationToFile.class);

  private String latestExactlyValue;
  private String latestAtLeastValue;
  //for tests
  static boolean validationDone = false;

  @NotNull
  private String maxTuplesTotal;

  List<String> exactlyList;
  List<String> atLeastList;

  @InputPortFieldAnnotation(optional = true)
  public final transient DefaultInputPort<byte[]> input = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] tuple) {}
  };

  public final transient DefaultInputPort<byte[]> topicExactlyInput = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] tuple)
    {
      String message = new String(tuple);
      latestExactlyValue = message;
      if (exactlyList == null) {
        exactlyList = new ArrayList<>();
      }
      exactlyList.add(message);

      processTuple(tuple);
    }

    @Override
    public StreamCodec<byte[]> getStreamCodec()
    {
      if (ValidationToFile.this.streamCodec == null) {
        return super.getStreamCodec();
      } else {
        return streamCodec;
      }
    }
  };

  public final transient DefaultInputPort<byte[]> topicAtLeastInput = new DefaultInputPort<byte[]>()
  {
    @Override
    public void process(byte[] tuple)
    {
      String message = new String(tuple);
      latestAtLeastValue = message;
      if (atLeastList == null) {
        atLeastList = new ArrayList<>();
      }
      atLeastList.add(message);

      processTuple(tuple);
    }

    @Override
    public StreamCodec<byte[]> getStreamCodec()
    {
      if (ValidationToFile.this.streamCodec == null) {
        return super.getStreamCodec();
      } else {
        return streamCodec;
      }
    }
  };

  @Override
  protected byte[] getBytesForTuple(byte[] tuple)
  {
    if (latestExactlyValue != null && latestAtLeastValue != null) {
      if (latestExactlyValue.equals(maxTuplesTotal) && latestAtLeastValue.equals(maxTuplesTotal)) {
        Set<String> exactlySet = new HashSet<>(exactlyList);
        Set<String> atLeastSet = new HashSet<>(atLeastList);

        int numDuplicatesExactly = exactlyList.size() - exactlySet.size();
        int numDuplicatesAtLeast = atLeastList.size() - atLeastSet.size();
        LOG.info("Duplicates: exactly-once: " + numDuplicatesExactly + ", at-least-once: " + numDuplicatesAtLeast);
        validationDone = true;
        return ("Duplicates: exactly-once: " + numDuplicatesExactly + ", at-least-once: " + numDuplicatesAtLeast).getBytes();
      } else {
        return new byte[0];
      }
    } else {
      return new byte[0];

    }
  }

  public String getMaxTuplesTotal()
  {
    return maxTuplesTotal;
  }

  public void setMaxTuplesTotal(String maxTuplesTotal)
  {
    this.maxTuplesTotal = maxTuplesTotal;
  }
}
