/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.demos.dimensions.generic;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.contrib.parser.AbstractCsvParser.Field;
import com.datatorrent.lib.stream.DevNull;

import org.apache.hadoop.conf.Configuration;
import java.util.ArrayList;
import java.util.Map;

@ApplicationAnnotation(name="CsvToMapParserTestApplication")
public class CsvToMapParserTestApplication implements StreamingApplication
{

  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);

    KafkaSinglePortByteArrayInputOperator kafkaStringInput = dag.addOperator("KafkaStringInput", new KafkaSinglePortByteArrayInputOperator());
    CsvToMapParser parser = dag.addOperator("CsvToMapParser", CsvToMapParser.class);

    ArrayList<CsvToMapParser.Field> fields= new ArrayList<CsvToMapParser.Field>();
    Field field1 = new Field();
    field1.setName("publisherId");
    field1.setType("INTEGER");
    fields.add(field1);
    Field field2 = new Field();
    field2.setName("advertiserId");
    field2.setType("INTEGER");
    fields.add(field2);
    Field field3 = new Field();
    field3.setName("adUnit");
    field3.setType("INTEGER");
    fields.add(field3);
    Field field4 = new Field();
    field4.setName("timestamp");
    field4.setType("LONG");
    fields.add(field4);
    Field field5 = new Field();
    field5.setName("cost");
    field5.setType("DOUBLE");
    fields.add(field5);
    Field field6 = new Field();
    field6.setName("revenue");
    field6.setType("DOUBLE");
    fields.add(field6);
    Field field7 = new Field();
    field7.setName("impressions");
    field7.setType("LONG");
    fields.add(field7);
    Field field8 = new Field();
    field8.setName("clicks");
    field8.setType("LONG");
    fields.add(field8);

    parser.setFields(fields);
    parser.setIsHeader(false);
    parser.setFieldDelimiter(',');
    parser.setLineDelimiter("\n");
    @SuppressWarnings("unchecked")
    DevNull<Map<String,Object>> devNull = dag.addOperator("DevNull", DevNull.class);

    dag.addStream("Kafka2Parser", kafkaStringInput.outputPort, parser.input);
    dag.addStream("Parser2DevNull",parser.output,devNull.data);
  }

}
