/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr;

import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SimpleQueryDeserializer extends CustomQueryDeserializer
{
  private static final Logger logger = LoggerFactory.getLogger(SimpleQueryDeserializer.class);
  private ObjectMapper om = new ObjectMapper();

  public SimpleQueryDeserializer()
  {
  }

  @Override
  public Query deserialize(String json)
  {
    try {
      return om.readValue(json, this.getQueryClazz());
    }
    catch(IOException ex) {
      throw new RuntimeException(ex);
      //logger.error("{}", ex);
      //return null;
    }
  }
}
