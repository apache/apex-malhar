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
public class SimpleResultSerializer implements CustomResultSerializer
{
  private ObjectMapper om = new ObjectMapper();

  public SimpleResultSerializer()
  {
  }

  @Override
  public String serialize(Result result)
  {
    try {
      return om.writeValueAsString(result);
    }
    catch(IOException ex) {
      return null;
    }
  }
}
