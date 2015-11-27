/**
 * Copyright (c) 2015 DataTorrent, Inc.
 * All rights reserved.
 */
package com.datatorrent.contrib.dimensions;

import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator.Unifier;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.appdata.schemas.DataResultDimensional;
import com.datatorrent.lib.appdata.schemas.Result;

/**
 * This is a QueryResult unifier for AbstractAppDataDimensionStoreHDHT for port queryResult.
 *
 * The unifier filters out all the data which are having empty query result data.
 *
 * This is specially useful when Store is partitioned and Queries goes to all the stores but
 * only one store is going to hold the data for the actual results.
 */
public class DimensionStoreHDHTNonEmptyQueryResultUnifier extends BaseOperator implements Unifier<String>
{

  /**
   * Output port that emits only non-empty dataResults.
   */
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<String>();

  @Override
  public void process(String tuple)
  {
    JSONObject jo = null;

    try {
      jo = new JSONObject(tuple);
      if (jo.getString(Result.FIELD_TYPE).equals(DataResultDimensional.TYPE)) {
        JSONArray dataArray = jo.getJSONArray(Result.FIELD_DATA);
        if ((dataArray != null) && (dataArray.length() != 0)) {
          output.emit(tuple);
        }
      } else {
        output.emit(tuple);
      }
    } catch (JSONException ex) {
      throw new RuntimeException(ex);
    }
  }
}
