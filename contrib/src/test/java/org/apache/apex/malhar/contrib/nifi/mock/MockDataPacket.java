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
package org.apache.apex.malhar.contrib.nifi.mock;

import java.io.InputStream;
import java.util.Map;

import org.apache.nifi.remote.protocol.DataPacket;

public class MockDataPacket implements DataPacket
{

  private final Map<String, String> attributes;
  private final InputStream inputStream;
  private final long size;

  public MockDataPacket(Map<String, String> attributes, InputStream inputStream, long size)
  {
    this.attributes = attributes;
    this.inputStream = inputStream;
    this.size = size;
  }

  @Override
  public Map<String, String> getAttributes()
  {
    return attributes;
  }

  @Override
  public InputStream getData()
  {
    return inputStream;
  }

  @Override
  public long getSize()
  {
    return size;
  }

}
