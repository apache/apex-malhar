/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr;

import java.io.IOException;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SimpleDataSerializer implements CustomDataSerializer
{
  private ObjectMapper om = new ObjectMapper();

  public SimpleDataSerializer()
  {
  }

  @Override
  public String serialize(Result result)
  {
    try {
      return om.writeValueAsString(result);
    }
    catch(IOException ex) {
      ex.printStackTrace();
      return null;
    }
  }
}
