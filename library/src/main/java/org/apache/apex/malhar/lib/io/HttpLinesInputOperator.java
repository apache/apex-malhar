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
package org.apache.apex.malhar.lib.io;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.sun.jersey.api.client.ClientResponse;

/**
 * Incoming data is interpreted as lines of plain text and each tuple output is a line in the content.
 * <p></p>
 * @displayName HTTP Lines Input
 * @category Input
 * @tags http
 *
 * @since 0.9.4
 */
@org.apache.hadoop.classification.InterfaceStability.Evolving
public class HttpLinesInputOperator extends AbstractHttpInputOperator<String>
{
  @Override
  public void processResponse(ClientResponse response) throws IOException
  {
    InputStream is = response.getEntity(java.io.InputStream.class);
    BufferedReader br = new BufferedReader(new InputStreamReader(is));
    try {
      String line;
      while ((line = br.readLine()) != null) {
        rawOutput.emit(line);
        outputPort.emit(line);
      }
    } finally {
      br.close();
    }
  }

}
