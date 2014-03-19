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
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * (entities on stream delimited by leading length)
 * Incoming data is interpreted as JSONObject and converted to {@link java.util.Map}.<br>
 * If second rawOutput is connected then content is streamed to this port as it is.
 * <br>
 *
 * @since 0.9.4
 */
public class HttpJsonChunksInputOperator extends AbstractHttpInputOperator<Map<String, String>>
{
  private static final Logger LOG = LoggerFactory.getLogger(HttpJsonChunksInputOperator.class);

  @Override
  public void processResponse(ClientResponse response) throws IOException
  {
    InputStream is = response.getEntity(java.io.InputStream.class);

    while (true) {
      ByteArrayOutputStream bos = new ByteArrayOutputStream();
      byte[] bytes = new byte[255];
      int bytesRead;
      while ((bytesRead = is.read(bytes)) != -1) {
        LOG.debug("read {} bytes", bytesRead);
        bos.write(bytes, 0, bytesRead);
        if (is.available() == 0 && bos.size() > 0) {
          // give chance to process what we have before blocking on read
          break;
        }
      }
      try {
        if (processBytes(bos.toByteArray())) {
          LOG.debug("End of chunked input stream.");
          response.close();
          break;
        }
      }
      catch (JSONException ex) {
        LOG.error("Caught JSON error:", ex);
      }
      if (bytesRead == -1) {
        LOG.error("Unexpected end of chunked input stream");
        response.close();
        break;
      }

      bos.reset();
    }
  }

  private boolean processBytes(byte[] bytes) throws IOException, JSONException
  {
    StringBuilder chunkStr = new StringBuilder();
    // hack: when line is a number we skip to next object instead of using it to read length chunk bytes
    List<String> lines = IOUtils.readLines(new ByteArrayInputStream(bytes));
    boolean endStream = false;
    int currentChunkLength = 0;
    for (String line : lines) {
      try {
        int nextLength = Integer.parseInt(line);
        if (nextLength == 0) {
          endStream = true;
          break;
        }
        // switch to next chunk
        processChunk(chunkStr, currentChunkLength);
        currentChunkLength = nextLength;

        //LOG.debug("chunk length: " + line);
      }
      catch (NumberFormatException e) {
        // add to chunk
        chunkStr.append(line);
        chunkStr.append("\n");
      }
    }
    // process current chunk, if any
    processChunk(chunkStr, currentChunkLength);
    return endStream;
  }

  /**
   * We are expecting a JSON object and converting one level of keys to a map, no further mapping is performed
   *
   * @param chunk
   * @param expectedLength
   * @throws JSONException
   */
  private void processChunk(StringBuilder chunk, int expectedLength) throws JSONException
  {
    if (expectedLength > 0 && chunk.length() > 0) {
      //LOG.debug("completed chunk: " + chunkStr);
      JSONObject json = new JSONObject(chunk.toString());
      chunk = new StringBuilder();
      Map<String, String> tuple = new HashMap<String, String>();
      Iterator<?> it = json.keys();
      while (it.hasNext()) {
        String key = (String)it.next();
        Object val = json.get(key);
        if (val != null) {
          String vstr = val.toString();
          tuple.put(key, vstr);
        }
      }
      if (!tuple.isEmpty()) {
        LOG.debug("Got: " + tuple);
        outputPort.emit(tuple);
        chunk.setLength(0);
      }
    }
    if (rawOutput.isConnected()) {
      rawOutput.emit(chunk.toString());
    }
  }

}
