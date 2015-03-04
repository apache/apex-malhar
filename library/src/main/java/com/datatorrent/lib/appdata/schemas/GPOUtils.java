/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Iterator;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GPOUtils
{
  public static GPOMutable deserialize(JSONObject dpou)
  {
    Iterator itr = dpou.keys();

    while(itr.hasNext()) {
      String key = (String) itr.next();

      boolean processed = true;

      //boolean

      /*try {

      }
      catch(JSONException ex) {
        processed = false;
      }*/

      //dpou.

      String valString;

      try {
        valString = dpou.getString(key);
      }
      catch(JSONException ex) {
        processed = false;
      }

      if(processed) {
        continue;
      }

      processed = true;

      //dpou.getDouble(key)
    }

    return null;
  }
}
