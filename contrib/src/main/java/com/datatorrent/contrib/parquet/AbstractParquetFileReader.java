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
package com.datatorrent.contrib.parquet;

import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.fs.Path;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.lib.io.fs.AbstractFileInputOperator;

import parquet.example.data.Group;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.example.GroupReadSupport;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

/**
 * Base implementation of ParquetFileReader. Reads Parquet files from input
 * directory using GroupReadSupport. Derived classes need to implement
 * {@link #convertGroup(Group)} method to convert Group to other type. Example
 * of such implementation is {@link ParquetFilePOJOReader} that converts Group
 * to POJO.
 *
 * @since 3.4.0
 */
public abstract class AbstractParquetFileReader<T> extends AbstractFileInputOperator<T>
{
  private transient ParquetReader<Group> reader;
  protected transient MessageType schema;
  /**
   * Parquet Schema as a string. E.g: message
   * com.datatorrent.contrib.parquet.eventsEventRecord {required INT32
   * event_id;required BINARY org_id (UTF8);required INT64 long_id;optional
   * BOOLEAN css_file_loaded;optional FLOAT float_val;optional DOUBLE
   * double_val;}
   */
  protected String parquetSchema;

  @Override
  public void setup(OperatorContext context)
  {
    super.setup(context);
    schema = MessageTypeParser.parseMessageType(parquetSchema);
  }

  /**
   * Opens the file to read using GroupReadSupport
   */
  @Override
  protected InputStream openFile(Path path) throws IOException
  {
    InputStream is = super.openFile(path);
    GroupReadSupport readSupport = new GroupReadSupport();
    readSupport.init(configuration, null, schema);
    reader = new ParquetReader<Group>(path, readSupport);
    return is;
  }

  /**
   * Reads next record in parquet file as a group. Returns null when end of file
   * is reached
   */
  @Override
  protected T readEntity() throws IOException
  {
    Group group = reader.read();
    if (group != null) {
      return convertGroup(group);
    }
    return null;
  }

  /**
   * Derived classes need to provide an implementation to convert a Parquet
   * Group to any other type. Each Parquet record is read as a <b>Group</b>
   * (parquet.example.data.Group) and is passed onto this method.
   * 
   * @param group
   *          Parquet record represented as a Group
   * @return object of type T
   */
  protected abstract T convertGroup(Group group);

  /**
   * Get Parquet Schema as a String
   * 
   * @return parquetSchema Parquet Schema as a string.
   */
  public String getParquetSchema()
  {
    return parquetSchema;
  }

  /**
   * Set Parquet Schema as a String
   * 
   * @param parquetSchema
   *          Parquet Schema as a string
   */
  public void setParquetSchema(String parquetSchema)
  {
    this.parquetSchema = parquetSchema;
  }

}
