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
package org.apache.apex.malhar.stream.sample.cookbook;

import java.util.List;

import org.apache.apex.malhar.lib.db.jdbc.JdbcFieldInfo;
import org.apache.apex.malhar.lib.db.jdbc.JdbcPOJOInputOperator;
import org.apache.apex.malhar.lib.db.jdbc.JdbcPOJOInsertOutputOperator;
import org.apache.apex.malhar.lib.db.jdbc.JdbcStore;
import org.apache.apex.malhar.lib.db.jdbc.JdbcTransactionalStore;
import org.apache.apex.malhar.lib.function.Function;
import org.apache.apex.malhar.lib.util.FieldInfo;
import org.apache.apex.malhar.lib.util.KeyValPair;
import org.apache.apex.malhar.lib.window.TriggerOption;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.Window;
import org.apache.apex.malhar.lib.window.WindowOption;
import org.apache.apex.malhar.lib.window.accumulation.Max;
import org.apache.apex.malhar.stream.api.ApexStream;
import org.apache.apex.malhar.stream.api.CompositeStreamTransform;
import org.apache.apex.malhar.stream.api.WindowedStream;
import org.apache.apex.malhar.stream.api.impl.StreamFactory;
import org.apache.hadoop.conf.Configuration;

import static java.sql.Types.DOUBLE;
import static java.sql.Types.INTEGER;

import com.google.common.collect.Lists;
import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;

import static org.apache.apex.malhar.stream.api.Option.Options.name;

/**
 * MaxPerKeyExamples Application from Beam
 *
 * @since 3.5.0
 */
@ApplicationAnnotation(name = "MaxPerKeyExamples")
public class MaxPerKeyExamples implements StreamingApplication
{

  /**
   *  A map function to extract the mean temperature from {@link InputPojo}.
   */
  public static class ExtractTempFn implements Function.MapFunction<InputPojo, KeyValPair<Integer, Double>>
  {
    @Override
    public KeyValPair<Integer, Double> f(InputPojo row)
    {
      Integer month = row.getMonth();
      Double meanTemp = row.getMeanTemp();
      return new KeyValPair<Integer, Double>(month, meanTemp);
    }
  }


  /**
   * A map function to format output to {@link OutputPojo}.
   */
  public static class FormatMaxesFn implements Function.MapFunction<Tuple.WindowedTuple<KeyValPair<Integer, Double>>, OutputPojo>
  {
    @Override
    public OutputPojo f(Tuple.WindowedTuple<KeyValPair<Integer, Double>> input)
    {
      OutputPojo row = new OutputPojo();
      row.setMonth(input.getValue().getKey());
      row.setMeanTemp(input.getValue().getValue());
      return row;
    }
  }

  /**
   * A composite transformation to perform three tasks:
   * 1. extract the month and its mean temperature from input pojo.
   * 2. find the maximum mean temperature for every month.
   * 3. format the result to a output pojo object.
   */
  public static class MaxMeanTemp extends CompositeStreamTransform<WindowedStream<InputPojo>, WindowedStream<OutputPojo>>
  {
    @Override
    public WindowedStream<OutputPojo> compose(WindowedStream<InputPojo> rows)
    {
      // InputPojo... => <month, meanTemp> ...
      WindowedStream<KeyValPair<Integer, Double>> temps = rows.map(new ExtractTempFn(), name("ExtractTempFn"));

      // month, meanTemp... => <month, max mean temp>...
      WindowedStream<Tuple.WindowedTuple<KeyValPair<Integer, Double>>> tempMaxes =
          temps.accumulateByKey(new Max<Double>(),
          new Function.ToKeyValue<KeyValPair<Integer, Double>, Integer, Double>()
            {
              @Override
              public Tuple<KeyValPair<Integer, Double>> f(KeyValPair<Integer, Double> input)
              {
                return new Tuple.WindowedTuple<KeyValPair<Integer, Double>>(Window.GlobalWindow.INSTANCE, input);
              }
            }, name("MaxPerMonth"));

      // <month, max>... => OutputPojo...
      WindowedStream<OutputPojo> results = tempMaxes.map(new FormatMaxesFn(), name("FormatMaxesFn"));

      return results;
    }
  }

  /**
   * Method to set field info for {@link JdbcPOJOInputOperator}.
   * @return
   */
  private List<FieldInfo> addInputFieldInfos()
  {
    List<FieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new FieldInfo("MONTH", "month", FieldInfo.SupportType.INTEGER));
    fieldInfos.add(new FieldInfo("DAY", "day", FieldInfo.SupportType.INTEGER));
    fieldInfos.add(new FieldInfo("YEAR", "year", FieldInfo.SupportType.INTEGER));
    fieldInfos.add(new FieldInfo("MEANTEMP", "meanTemp", FieldInfo.SupportType.DOUBLE));
    return fieldInfos;
  }

  /**
   * Method to set field info for {@link JdbcPOJOInsertOutputOperator}.
   * @return
   */
  private List<JdbcFieldInfo> addOutputFieldInfos()
  {
    List<JdbcFieldInfo> fieldInfos = Lists.newArrayList();
    fieldInfos.add(new JdbcFieldInfo("MONTH", "month", JdbcFieldInfo.SupportType.INTEGER, INTEGER));
    fieldInfos.add(new JdbcFieldInfo("MEANTEMP", "meanTemp", JdbcFieldInfo.SupportType.DOUBLE, DOUBLE));
    return fieldInfos;
  }


  /**
   * Populate the dag using High-Level API.
   * @param dag
   * @param conf
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    JdbcPOJOInputOperator jdbcInput = new JdbcPOJOInputOperator();
    jdbcInput.setFieldInfos(addInputFieldInfos());

    JdbcStore store = new JdbcStore();
    jdbcInput.setStore(store);

    JdbcPOJOInsertOutputOperator jdbcOutput = new JdbcPOJOInsertOutputOperator();
    jdbcOutput.setFieldInfos(addOutputFieldInfos());
    JdbcTransactionalStore outputStore = new JdbcTransactionalStore();
    jdbcOutput.setStore(outputStore);

    // Create stream that reads from a Jdbc Input.
    ApexStream<Object> stream = StreamFactory.fromInput(jdbcInput, jdbcInput.outputPort, name("jdbcInput"))

        // Apply window and trigger option to the stream.
        .window(new WindowOption.GlobalWindow(), new TriggerOption().accumulatingFiredPanes().withEarlyFiringsAtEvery(1))

        // Because Jdbc Input sends out a stream of Object, need to cast them to InputPojo.
        .map(new Function.MapFunction<Object, InputPojo>()
        {
          @Override
          public InputPojo f(Object input)
          {
            return (InputPojo)input;
          }
        }, name("ObjectToInputPojo"))

        // Plug in the composite transformation to the stream to calculate the maximum temperature for each month.
        .addCompositeStreams(new MaxMeanTemp())

        // Cast the resulted OutputPojo to Object for Jdbc Output to consume.
        .map(new Function.MapFunction<OutputPojo, Object>()
        {
          @Override
          public Object f(OutputPojo input)
          {
            return (Object)input;
          }
        }, name("OutputPojoToObject"))

        // Output the result to Jdbc Output.
        .endWith(jdbcOutput, jdbcOutput.input, name("jdbcOutput"));

    stream.populateDag(dag);

  }
}
