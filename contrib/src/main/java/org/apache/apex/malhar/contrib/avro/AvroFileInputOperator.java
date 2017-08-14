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
package org.apache.apex.malhar.contrib.avro;

import java.io.IOException;
import java.io.InputStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.io.fs.AbstractFileInputOperator;
import org.apache.apex.malhar.lib.wal.FSWindowDataManager;
import org.apache.avro.AvroRuntimeException;
import org.apache.avro.file.DataFileStream;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.apache.hadoop.classification.InterfaceStability;
import org.apache.hadoop.fs.Path;

import com.google.common.annotations.VisibleForTesting;

import com.datatorrent.api.AutoMetric;
import com.datatorrent.api.DefaultOutputPort;

/**
 * <p>
 * Avro File Input Operator
 * </p>
 * A specific implementation of the AbstractFileInputOperator to read Avro
 * container files.<br>
 * No need to provide schema,its inferred from the file<br>
 * This operator emits a GenericRecord based on the schema derived from the
 * input file<br>
 * Users can add the {@link FSWindowDataManager}
 * to ensure exactly once semantics with a HDFS backed WAL.
 *
 * @displayName AvroFileInputOperator
 * @category Input
 * @tags fs, file,avro, input operator
 *
 * @since 3.4.0
 */
@InterfaceStability.Evolving
public class AvroFileInputOperator extends AbstractFileInputOperator<GenericRecord>
{

  private transient long offset = 0L;

  @AutoMetric
  @VisibleForTesting
  int recordCount = 0;

  @AutoMetric
  @VisibleForTesting
  int errorCount = 0;

  private transient DataFileStream<GenericRecord> avroDataStream;

  public final transient DefaultOutputPort<GenericRecord> output = new DefaultOutputPort<GenericRecord>();

  public final transient DefaultOutputPort<String> completedFilesPort = new DefaultOutputPort<String>();

  public final transient DefaultOutputPort<String> errorRecordsPort = new DefaultOutputPort<String>();

  /**
   * Returns a input stream given a file path
   *
   * @param path
   * @return InputStream
   * @throws IOException
   */
  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    if (is != null) {
      DatumReader<GenericRecord> datumReader = new GenericDatumReader<GenericRecord>();
      avroDataStream = new DataFileStream<GenericRecord>(is, datumReader);
      datumReader.setSchema(avroDataStream.getSchema());
    }
    return is;
  }

  /**
   * Reads a GenericRecord from the given input stream<br>
   * Emits the FileName,Offset,Exception on the error port if its connected
   *
   * @return GenericRecord
   */
  @Override
  protected GenericRecord readEntity() throws IOException
  {
    GenericRecord record = null;

    record = null;

    try {
      if (avroDataStream != null && avroDataStream.hasNext()) {
        offset++;
        record = avroDataStream.next();
        recordCount++;
        return record;
      }
    } catch (AvroRuntimeException are) {
      LOG.error("Exception in parsing record for file - " + super.currentFile + " at offset - " + offset, are);
      if (errorRecordsPort.isConnected()) {
        errorRecordsPort.emit("FileName:" + super.currentFile + ", Offset:" + offset);
      }
      errorCount++;
      throw new AvroRuntimeException(are);
    }
    return record;
  }

  /**
   * Closes the input stream If the completed files port is connected, the
   * completed file is emitted from this port
   */
  @Override
  protected void closeFile(InputStream is) throws IOException
  {
    String fileName = super.currentFile;

    if (avroDataStream != null) {
      avroDataStream.close();
    }

    super.closeFile(is);

    if (completedFilesPort.isConnected()) {
      completedFilesPort.emit(fileName);
    }
    offset = 0;
  }

  @Override
  protected void emit(GenericRecord tuple)
  {
    if (tuple != null) {
      output.emit(tuple);
    }
  }

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    errorCount = 0;
    recordCount = 0;
  }

  private static final Logger LOG = LoggerFactory.getLogger(AvroFileInputOperator.class);

}
