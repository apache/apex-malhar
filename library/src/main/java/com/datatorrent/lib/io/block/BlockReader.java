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
package com.datatorrent.lib.io.block;

import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.fs.FileSystem;

import com.datatorrent.api.AutoMetric;

/**
 * BlockReader extends {@link FSSliceReader} to accept case insensitive uri
 */
public class BlockReader extends FSSliceReader
{
  @AutoMetric
  private long bytesRead;

  protected String uri;

  @Override
  public void beginWindow(long windowId)
  {
    super.beginWindow(windowId);
    bytesRead = 0;
  }

  @Override
  protected FileSystem getFSInstance() throws IOException
  {
    return FileSystem.newInstance(URI.create(uri), configuration);
  }

  /**
   * Sets the uri
   *
   * @param uri of form hdfs://hostname:port/path/to/input
   */
  public void setUri(String uri)
  {
    this.uri = uri;
  }

  public String getUri()
  {
    return uri;
  }

}
