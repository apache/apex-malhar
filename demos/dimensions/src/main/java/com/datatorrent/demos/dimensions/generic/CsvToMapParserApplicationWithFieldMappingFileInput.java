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
import com.datatorrent.common.util.DTThrowable;
import com.datatorrent.lib.stream.DevNull;
import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import java.util.Map;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@ApplicationAnnotation(name = "CsvToMapParserApplicationWithFieldMappingFileInput")
public class CsvToMapParserApplicationWithFieldMappingFileInput implements StreamingApplication
{
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {
    dag.setAttribute(DAG.STREAMING_WINDOW_SIZE_MILLIS, 1000);

    KafkaSinglePortByteArrayInputOperator kafkaStringInput = dag.addOperator("KafkaStringInput", new KafkaSinglePortByteArrayInputOperator());
    CsvToMapParser parser = dag.addOperator("CsvParser", CsvToMapParser.class);
    String filepath = conf.get("dt.application.CsvParserFileMappingInputApplication.operator.CsvParser.fieldmappingFile");
    parser.setFieldmappingFile(filepath);
    createFieldMappingFile(filepath);
    parser.setIsHeader(false);
    parser.setFieldDelimiter(',');
    parser.setLineDelimiter("\n");
    @SuppressWarnings("unchecked")
    DevNull<Map<String, Object>> devNull = dag.addOperator("DevNull", DevNull.class);

    dag.addStream("Kafka2Parser", kafkaStringInput.outputPort, parser.input);
    dag.addStream("Parser2DevNull", parser.output, devNull.data);
  }

  public void createFieldMappingFile(String filepath)
  {
    FileSystem hdfs = null;
    //Creating a file in HDFS
    Path newFilePath = new Path(filepath);
    try {
      hdfs = FileSystem.get(new Configuration());
      hdfs.createNewFile(newFilePath);
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);
    }
    //Writing data to a HDFS file
    StringBuilder sb = new StringBuilder();
    sb.append("publisherId");
    sb.append(":");
    sb.append("INTEGER");
    sb.append("\n");
    sb.append("advertiserId");
    sb.append(":");
    sb.append("INTEGER");
    sb.append("\n");
    sb.append("adUnit");
    sb.append(":");
    sb.append("INTEGER");
    sb.append("\n");
    sb.append("timestamp");
    sb.append(":");
    sb.append("LONG");
    sb.append("\n");
    sb.append("cost");
    sb.append(":");
    sb.append("DOUBLE");
    sb.append("\n");
    sb.append("revenue");
    sb.append(":");
    sb.append("DOUBLE");
    sb.append("\n");
    sb.append("impressions");
    sb.append(":");
    sb.append("LONG");
    sb.append("\n");
    sb.append("clicks");
    sb.append(":");
    sb.append("LONG");
    sb.append("\n");

    byte[] byt = sb.toString().getBytes();
    try {
      FSDataOutputStream fsOutStream = hdfs.create(newFilePath);
      fsOutStream.write(byt);
      fsOutStream.close();
    }
    catch (IOException ex) {
      DTThrowable.rethrow(ex);

    }
    logger.debug("Written data to HDFS file.");
  }

  private static final Logger logger = LoggerFactory.getLogger(CsvToMapParserApplicationWithFieldMappingFileInput.class);

}
