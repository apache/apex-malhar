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
package org.apache.apex.malhar.stream.sample.complete;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.apex.malhar.lib.db.jdbc.JdbcFieldInfo;
import org.apache.apex.malhar.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import org.apache.apex.malhar.lib.db.jdbc.JdbcTransactionalStore;
import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.Option;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import static java.sql.Types.VARCHAR;

/**
 * Beam StreamingWordExtract Example.
 *
 * @since 3.5.0
 */
@ApplicationAnnotation(name = "StreamingWordExtract")
public class StreamingWordExtract implements StreamingApplication
{
  private static int wordCount = 0; // A counter to count number of words have been extracted.
  private static int entriesMapped = 0; // A counter to count number of entries have been mapped.

  public int getWordCount()
  {
    return wordCount;
  }

  public int getEntriesMapped()
  {
    return entriesMapped;
  }

  /**
   * A MapFunction that tokenizes lines of text into individual words.
   */
  public static class ExtractWords implements Function.FlatMapFunction<String, String>
  {
    @Override
    public Iterable<String> f(String input)
    {
      List<String> result = new ArrayList<>(Arrays.asList(input.split("[^a-zA-Z0-9']+")));
      wordCount += result.size();
      return result;
    }
  }


  /**
   * A MapFunction that uppercases a word.
   */
  public static class Uppercase implements Function.MapFunction<String, String>
  {
    @Override
    public String f(String input)
    {
      return input.toUpperCase();
    }
  }


  /**
   * A filter function to filter out empty strings.
   */
  public static class EmptyStringFilter implements Function.FilterFunction<String>
  {
    @Override
    public boolean f(String input)
    {
      return !input.isEmpty();
    }
  }


  /**
   * A map function to map the result string to a pojo entry.
   */
  public static class PojoMapper implements Function.MapFunction<String, Object>
  {

    @Override
    public Object f(String input)
    {
      PojoEvent pojo = new PojoEvent();
      pojo.setStringValue(input);
      entriesMapped++;
      return pojo;
    }
  }

  /**
   * Add field infos to the {@link JdbcPOJOInsertOutputOperator}.
   */
  private static List<JdbcFieldInfo> addFieldInfos()
  {
    List<JdbcFieldInfo> fieldInfos = new ArrayList<>();
    fieldInfos.add(new JdbcFieldInfo("STRINGVALUE", "stringValue", JdbcFieldInfo.SupportType.STRING, VARCHAR));
    return fieldInfos;
  }

  /**
   * Populate dag with High-Level API.
   * @param dag
   * @param conf
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JdbcPOJOInsertOutputOperator jdbcOutput = new JdbcPOJOInsertOutputOperator();
    jdbcOutput.setFieldInfos(addFieldInfos());
    JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
    jdbcOutput.setStore(outputStore);
    jdbcOutput.setTablename("TestTable");

    // Create a stream reading from a folder.
    ApexStream<String> stream = StreamFactory.fromFolder("./src/test/resources/data");

    // Extract all the words from the input line of text.
    stream.flatMap(new ExtractWords())

        // Filter out the empty strings.
        .filter(new EmptyStringFilter())

        // Change every word to uppercase.
        .map(new Uppercase())

        // Map the resulted word to a Pojo entry.
        .map(new PojoMapper())

        // Output the entries to JdbcOutput and insert them into a table.
        .endWith(jdbcOutput, jdbcOutput.input, Option.Options.name("jdbcOutput"));

    stream.populateDag(dag);
  }
}
