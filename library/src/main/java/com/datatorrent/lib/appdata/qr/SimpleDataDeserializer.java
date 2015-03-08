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
public class SimpleDataDeserializer extends CustomDataDeserializer
{
  private static final Logger logger = LoggerFactory.getLogger(SimpleDataDeserializer.class);
  private ObjectMapper om = new ObjectMapper();

  public SimpleDataDeserializer()
  {
  }

  @Override
  public Data deserialize(String json)
  {
    Data data;

    try {
      data = om.readValue(json, this.getDataClazz());
    }
    catch(IOException ex) {
      //throw new RuntimeException(ex);
      logger.error("{}", ex);
      return null;
    }

    return data;
  }
}
