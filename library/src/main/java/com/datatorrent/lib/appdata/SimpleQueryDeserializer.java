/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata;

import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SimpleQueryDeserializer extends CustomQueryDeserializer
{
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
      return null;
    }
  }
}
