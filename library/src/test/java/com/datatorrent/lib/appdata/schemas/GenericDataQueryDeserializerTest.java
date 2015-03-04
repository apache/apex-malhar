/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericDataQueryDeserializerTest
{
  private static final Logger logger = LoggerFactory.getLogger(GenericDataQueryDeserializerTest.class);

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
