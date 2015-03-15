/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada;

import com.datatorrent.ada.counters.CountersSchema;
import com.datatorrent.ada.counters.CountersSchema.CountersSchemaValues;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import com.datatorrent.lib.appdata.schemas.Type;
import junit.framework.Assert;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class CountersSchemaDeserializerTest
{
  private static final Logger logger = LoggerFactory.getLogger(CountersSchemaDeserializerTest.class);

  @Test
  public void deserializeTest() throws Exception
  {
    final String version = "1.0";
    final String user = "test";
    final String appName = "appName";
    final String operator = "operator";

    JSONObject test = new JSONObject();
    test.put(Data.FIELD_TYPE, CountersSchema.TYPE);
    test.put(CountersSchema.FIELD_VERSION, version);
    test.put(CountersSchema.FIELD_USER, user);
    test.put(CountersSchema.FIELD_APP_NAME, appName);
    test.put(CountersSchema.FIELD_LOGICAL_OPERATOR_NAME, operator);

    JSONArray aggregationArray = new JSONArray();
    aggregationArray.put(CountersSchema.AGGREGATION_COUNT);
    aggregationArray.put(CountersSchema.AGGREGATION_MAX);
    aggregationArray.put(CountersSchema.AGGREGATION_MIN);
    aggregationArray.put(CountersSchema.AGGREGATION_SUM);

    JSONArray valuesArray = new JSONArray();

    final String value1 = "value1";
    final String value1Type = "long";
    JSONObject value1O = new JSONObject();
    value1O.put(CountersSchema.FIELD_VALUE_NAME, value1);
    value1O.put(CountersSchema.FIELD_VALUE_AGGREGATIONS, aggregationArray);
    value1O.put(CountersSchema.FIELD_VALUE_TYPE, value1Type);
    valuesArray.put(value1O);

    final String value2 = "value2";
    final String value2Type = "double";
    JSONObject value2O = new JSONObject();
    value2O.put(CountersSchema.FIELD_VALUE_NAME, value2);
    value2O.put(CountersSchema.FIELD_VALUE_AGGREGATIONS, aggregationArray);
    value2O.put(CountersSchema.FIELD_VALUE_TYPE, value2Type);
    valuesArray.put(value2O);

    test.put(CountersSchema.FIELD_VALUES, valuesArray);

    JSONArray timeBuckets = new JSONArray();
    timeBuckets.put(CountersSchema.TIME_BUCKET_MINUTE);
    timeBuckets.put(CountersSchema.TIME_BUCKET_HOUR);
    timeBuckets.put(CountersSchema.TIME_BUCKET_DAY);
    test.put(CountersSchema.FIELD_TIME_BUCKETS, timeBuckets);

    String jsonString = test.toString(2);
    logger.debug("{}", jsonString);

    DataDeserializerFactory qb = new DataDeserializerFactory(CountersSchema.class);
    CountersSchema dq = (CountersSchema) qb.deserialize(jsonString);

    Assert.assertEquals("Types must match.", CountersSchema.TYPE, dq.getType());
    Assert.assertEquals("Versions must match.", version, dq.getVersion());
    Assert.assertEquals("Users must match.", user, dq.getUser());
    Assert.assertEquals("Appnames must match.", appName, dq.getAppName());
    Assert.assertEquals("logicalOperatorNames must match", operator, dq.getLogicalOperatorName());

    List<CountersSchemaValues> schemaValues = dq.getValues();

    CountersSchemaValues csv1 = schemaValues.get(0);
    List<String> aggregations1 = csv1.getAggregations();

    Assert.assertEquals("Value name.", value1, csv1.getName());
    Assert.assertEquals("AggregationType must match.", CountersSchema.AGGREGATION_COUNT, aggregations1.get(0));
    Assert.assertEquals("AggregationType must match.", CountersSchema.AGGREGATION_MAX, aggregations1.get(1));
    Assert.assertEquals("AggregationType must match.", CountersSchema.AGGREGATION_MIN, aggregations1.get(2));
    Assert.assertEquals("AggregationType must match.", CountersSchema.AGGREGATION_SUM, aggregations1.get(3));
    Assert.assertEquals("Types must match", Type.LONG.getName(), csv1.getType());

    CountersSchemaValues csv2 = schemaValues.get(1);
    List<String> aggregations2 = csv2.getAggregations();

    Assert.assertEquals("Value name.", value2, csv2.getName());
    Assert.assertEquals("AggregationType must match.", CountersSchema.AGGREGATION_COUNT, aggregations2.get(0));
    Assert.assertEquals("AggregationType must match.", CountersSchema.AGGREGATION_MAX, aggregations2.get(1));
    Assert.assertEquals("AggregationType must match.", CountersSchema.AGGREGATION_MIN, aggregations2.get(2));
    Assert.assertEquals("AggregationType must match.", CountersSchema.AGGREGATION_SUM, aggregations2.get(3));
    Assert.assertEquals("Types must match", Type.DOUBLE.getName(), csv2.getType());

    List<String> timeBucketsStrings = dq.getTimeBuckets();

    Assert.assertEquals("Bucket must match.", CountersSchema.TIME_BUCKET_MINUTE, timeBucketsStrings.get(0));
    Assert.assertEquals("Bucket must match.", CountersSchema.TIME_BUCKET_HOUR, timeBucketsStrings.get(1));
    Assert.assertEquals("Bucket must match.", CountersSchema.TIME_BUCKET_DAY, timeBucketsStrings.get(2));
  }
}
