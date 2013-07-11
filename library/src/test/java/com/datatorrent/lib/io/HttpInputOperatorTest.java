/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io;

import com.datatorrent.lib.io.HttpInputOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import junit.framework.Assert;
import org.apache.commons.io.IOUtils;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.Server;
import org.mortbay.jetty.handler.AbstractHandler;
import org.junit.Test;

public class HttpInputOperatorTest
{
  @Test
  @SuppressWarnings("SleepWhileInLoop")
  public void testHttpInputModule() throws Exception
  {

    final List<String> receivedMessages = new ArrayList<String>();
    Handler handler = new AbstractHandler()
    {
      int responseCount = 0;

      @Override
      public void handle(String string, HttpServletRequest request, HttpServletResponse response, int i) throws IOException, ServletException
      {
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        IOUtils.copy(request.getInputStream(), bos);
        receivedMessages.add(new String(bos.toByteArray()));
        response.setContentType("application/json");
        response.setStatus(HttpServletResponse.SC_OK);
        response.setHeader("Transfer-Encoding", "chunked");
        try {
          JSONObject json = new JSONObject();
          json.put("responseId", "response" + ++responseCount);
          byte[] bytes = json.toString().getBytes();
          response.getOutputStream().println(bytes.length);
          response.getOutputStream().write(bytes);
          response.getOutputStream().println();
          response.getOutputStream().println(0);
          response.getOutputStream().flush();
        }
        catch (JSONException e) {
          response.sendError(HttpServletResponse.SC_INTERNAL_SERVER_ERROR, "Error generating response: " + e.toString());
        }

        ((Request)request).setHandled(true);
      }
    };

    Server server = new Server(0);
    server.setHandler(handler);
    server.start();

    String url = "http://localhost:" + server.getConnectors()[0].getLocalPort() + "/somecontext";
    //String url = "http://localhost:8080/channel/mobile/phoneLocationQuery";

    final HttpInputOperator operator = new HttpInputOperator();

    CollectorTestSink sink = new CollectorTestSink();

    operator.outputPort.setSink(sink);
    operator.setName("testHttpInputNode");
    operator.setUrl(new URI(url));

    operator.setup(null);
    operator.activate(null);

//    sink.waitForResultCount(1, 3000);
    int timeoutMillis = 3000;
    while (sink.collectedTuples.isEmpty() && timeoutMillis > 0) {
      operator.emitTuples();
      timeoutMillis -= 20;
      Thread.sleep(20);
    }

    Assert.assertTrue("tuple emmitted", sink.collectedTuples.size() > 0);

    Map<String, String> tuple = (Map<String, String>)sink.collectedTuples.get(0);
    Assert.assertEquals("", tuple.get("responseId"), "response1");

    operator.deactivate();
    operator.teardown();
    server.stop();

  }
}
