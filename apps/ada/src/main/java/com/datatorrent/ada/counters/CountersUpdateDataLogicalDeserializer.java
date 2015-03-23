/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada.counters;

import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.gpo.GPOMutable;
import com.datatorrent.lib.appdata.gpo.GPOUtils;
import com.datatorrent.lib.appdata.qr.CustomDataDeserializer;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.schemas.FieldsDescriptor;
import com.datatorrent.lib.appdata.schemas.Type;
import com.google.common.collect.Maps;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class CountersUpdateDataLogicalDeserializer extends CustomDataDeserializer
{
  private static final Logger logger = LoggerFactory.getLogger(CountersUpdateDataLogicalDeserializer.class);

  public CountersUpdateDataLogicalDeserializer()
  {
  }

  @Override
  public Data deserialize(String json, Object context)
  {
    try {
      return deserializeHelper(json);
    }
    catch(Exception ex) {
      logger.error("There was an error deserializing: " + json, ex);
      return null;
    }
  }

  private Data deserializeHelper(String json) throws Exception
  {
    JSONObject jo = new JSONObject(json);

    CountersUpdateDataLogical cud = new CountersUpdateDataLogical();
    cud.setType(jo.getString(Data.FIELD_TYPE));
    cud.setUser(jo.getString(CountersData.FIELD_USER));
    cud.setAppID(jo.getString(CountersUpdateDataLogical.FIELD_APP_ID));
    cud.setVersion(jo.getString(CountersUpdateDataLogical.FIELD_VERSION));
    cud.setAppName(jo.getString(CountersData.FIELD_APP_NAME));
    cud.setLogicalOperatorName(jo.getString(CountersData.FIELD_LOGICAL_OPERATOR_NAME));
    cud.setWindowID(jo.getLong(CountersUpdateDataLogical.FIELD_WINDOW_ID));
    cud.setTime(jo.getLong(CountersUpdateDataLogical.FIELD_TIME));

    CountersSchema schema = CountersSchemaUtils.getSchema(cud);

    if(schema == null) {
      logger.error("No schema exists for application {}"
                   + " and logical operator {}",
                   cud.getAppName(),
                   cud.getLogicalOperatorName());
      return null;
    }

    JSONObject aggVals = jo.getJSONObject(CountersUpdateDataLogical.FIELD_LOGICAL_COUNTERS);

    Map<String, FieldsDescriptor> aggregateToFieldsDescriptor = schema.getAggregateToFieldDescriptor();
    Map<String, Set<String>> aggregateToFields = schema.getAggregateToFields();
    Map<String, List<String>> fieldToAggregators = schema.getFieldToAggregators();

    Map<String, GPOMutable> aggregatorToGPO = Maps.newHashMap();
    Iterator keyIterator = aggVals.keys();

    while(keyIterator.hasNext()) {
      String field = (String) keyIterator.next();

      List<String> aggregators = fieldToAggregators.get(field);
      JSONArray aggResults = aggVals.getJSONArray(field);

      for(int index = 0;
          index < aggregators.size();
          index++) {
        String aggregator = aggregators.get(index);
        FieldsDescriptor aggFieldsDescriptor = aggregateToFieldsDescriptor.get(aggregator);

        GPOMutable gpo = aggregatorToGPO.get(aggregator);

        if(gpo == null) {
          logger.debug("Aggregator name: {}", aggregator);
          gpo = new GPOMutable(aggFieldsDescriptor);
          aggregatorToGPO.put(aggregator, gpo);
        }

        Type aggType = aggFieldsDescriptor.getType(field);
        GPOUtils.setFieldFromJSON(gpo, aggType, field, aggResults, index);
      }
    }

    Map<String, GPOImmutable> aggregatorToGPOIm = Maps.newHashMap();

    for(Map.Entry<String, GPOMutable> entry: aggregatorToGPO.entrySet()) {
      aggregatorToGPOIm.put(entry.getKey(), new GPOImmutable(entry.getValue()));
    }

    aggregatorToGPOIm = Collections.unmodifiableMap(aggregatorToGPOIm);
    cud.setAggregateToVals(aggregatorToGPOIm);

    return cud;
  }
}
