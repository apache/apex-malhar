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
package com.datatorrent.demos.wordcount;

import java.io.UnsupportedEncodingException;

import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.io.fs.AbstractFileOutputOperator;

// write top N words and their frequencies to a file
//
public class WordCountWriter extends AbstractFileOutputOperator<Map<String, Object>>
{
  private static final Logger LOG = LoggerFactory.getLogger(WordCountWriter.class);
  private static final String charsetName = "UTF-8";
  private static final String nl = System.lineSeparator();

  private String fileName;    // current file name
  private transient final StringBuilder sb = new StringBuilder();

  @Override
  public void endWindow()
  {
    if (null != fileName) {
      requestFinalize(fileName);
    }
    super.endWindow();
  }

  // input is a singleton list [M] where M is a singleton map {fileName => L} where L is a
  // list of pairs: (word, frequency)
  //
  @Override
  protected String getFileName(Map<String, Object> tuple)
  {
    LOG.info("getFileName: tuple.size = {}", tuple.size());

    final Map.Entry<String, Object> entry = tuple.entrySet().iterator().next();
    fileName = entry.getKey();
    LOG.info("getFileName: fileName = {}", fileName);
    return fileName;
  }

  @Override
  protected byte[] getBytesForTuple(Map<String, Object> tuple)
  {
    LOG.info("getBytesForTuple: tuple.size = {}", tuple.size());

    // get first and only pair; key is the fileName and is ignored here
    final Map.Entry<String, Object> entry = tuple.entrySet().iterator().next();
    final List<WCPair> list = (List<WCPair>) entry.getValue();

    if (sb.length() > 0) {        // clear buffer
      sb.delete(0, sb.length());
    }

    for ( WCPair pair : list ) {
      sb.append(pair.word); sb.append(" : ");
      sb.append(pair.freq); sb.append(nl);
    }

    final String data = sb.toString();
    LOG.info("getBytesForTuple: data = {}", data);
    try {
      final byte[] result = data.getBytes(charsetName);
      return result;
    } catch (UnsupportedEncodingException ex) {
      throw new RuntimeException("Should never get here", ex);
    }
  }

}
