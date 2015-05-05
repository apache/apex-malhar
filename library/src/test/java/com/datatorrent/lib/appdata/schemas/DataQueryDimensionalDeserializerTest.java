/*
 * Copyright (c) 2015 DataTorrent, Inc. ALL Rights Reserved.
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

package com.datatorrent.lib.appdata.schemas;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataQueryDimensionalDeserializerTest
{
  private static final Logger logger = LoggerFactory.getLogger(DataQueryDimensionalDeserializerTest.class);

  @Test
  public void deserializeTest() throws Exception
  {
    String json = "{\"a\":10000000000,\"b\":\"a\",\"c\":1.0}";

    JSONObject jsonObject = new JSONObject(json);

    logger.debug("{}", jsonObject.get("a").getClass());
    logger.debug("{}", jsonObject.get("b").getClass());
    logger.debug("{}", jsonObject.get("c").getClass());
  }
}
