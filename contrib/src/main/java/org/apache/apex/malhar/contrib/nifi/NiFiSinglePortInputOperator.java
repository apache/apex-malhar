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
package org.apache.apex.malhar.contrib.nifi;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import org.apache.apex.malhar.lib.wal.WindowDataManager;
import org.apache.nifi.remote.client.SiteToSiteClient;
import org.apache.nifi.remote.protocol.DataPacket;
import org.apache.nifi.stream.io.StreamUtils;

/**
 * Input adapter operator which consumes data from NiFi and produces NiFiDataPackets
 * where each NiFiDataPacket contains a byte array of content and a Map of attributes.
 *
 * @displayName NiFi Input Operator
 * @category Messaging
 * @tags input operator
 *
 *
 * @since 3.4.0
 */
public class NiFiSinglePortInputOperator extends AbstractNiFiSinglePortInputOperator<NiFiDataPacket>
{

  // required by Kyro serialization
  private NiFiSinglePortInputOperator()
  {
    super(null, null);
  }

  /**
   *
   * @param siteToSiteBuilder the builder for a NiFi SiteToSiteClient
   * @param windowDataManager a WindowDataManager to save and load state for windows of tuples
   */
  public NiFiSinglePortInputOperator(final SiteToSiteClient.Builder siteToSiteBuilder,
      final WindowDataManager windowDataManager)
  {
    super(siteToSiteBuilder, windowDataManager);
  }

  @Override
  protected NiFiDataPacket createTuple(final DataPacket dataPacket) throws IOException
  {
    // read the data into a byte array and wrap it with the attributes into a NiFiDataPacket
    final InputStream inStream = dataPacket.getData();
    final byte[] data = new byte[(int)dataPacket.getSize()];
    StreamUtils.fillBuffer(inStream, data);

    final Map<String, String> attributes = dataPacket.getAttributes();
    return new StandardNiFiDataPacket(data, attributes);
  }

}
