/*
 *  Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package com.datatorrent.lib.io;

import com.sun.jersey.api.client.ClientResponse;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

/**
 * Incoming data is interpreted as lines of plain text and each tuple output is a line in the content
 * <br>
 *
 * @since 0.9.4
 */
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
    }
    finally {
      br.close();
    }
  }

}
