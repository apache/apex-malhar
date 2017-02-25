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
package org.apache.apex.examples.mrmonitor;

import java.io.IOException;

import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.ResponseHandler;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.BasicResponseHandler;
import org.apache.http.impl.client.DefaultHttpClient;

/**
 * <p>
 * Util class.
 * </p>
 *
 * @since 0.3.4
 */
public class MRUtil
{

  private static final Logger logger = LoggerFactory.getLogger(MRUtil.class);

  /**
   * This method returns the response content for a given url
   * @param url
   * @return
   */
  public static String getJsonForURL(String url)
  {
    HttpClient httpclient = new DefaultHttpClient();
    logger.debug(url);
    try {


      HttpGet httpget = new HttpGet(url);

      // Create a response handler
      ResponseHandler<String> responseHandler = new BasicResponseHandler();
      String responseBody;
      try {
        responseBody = httpclient.execute(httpget, responseHandler);

      } catch (ClientProtocolException e) {
        logger.debug(e.getMessage());
        return null;

      } catch (IOException e) {
        logger.debug(e.getMessage());
        return null;
      } catch (Exception e) {
        logger.debug(e.getMessage());
        return null;
      }
      return responseBody.trim();
    } finally {
      httpclient.getConnectionManager().shutdown();
    }
  }

  /**
   * This method returns the JSONObject for a given string
   * @param json
   * @return
   */
  public static JSONObject getJsonObject(String json)
  {
    try {
      JSONObject jsonObj = new JSONObject(json);
      return jsonObj;
    } catch (Exception e) {
      logger.debug("{}", e.getMessage());
      return null;
    }
  }

}
