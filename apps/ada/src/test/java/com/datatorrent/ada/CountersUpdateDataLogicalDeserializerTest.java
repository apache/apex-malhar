/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

package com.datatorrent.ada;

import com.datatorrent.ada.counters.CountersSchema;
import com.datatorrent.ada.counters.CountersSchemaUtils;
import com.datatorrent.ada.counters.CountersUpdateDataLogical;
import com.datatorrent.ada.dimensions.AggregatorType;
import com.datatorrent.lib.appdata.gpo.GPOImmutable;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.DataDeserializerFactory;
import junit.framework.Assert;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONObject;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class CountersUpdateDataLogicalDeserializerTest
{
  private static final Logger logger = LoggerFactory.getLogger(CountersUpdateDataLogicalDeserializerTest.class);

  @Test
  public void deserializeTest() throws Exception
  {
    final String version = "1.0";
    final String user = "test";
    final String appName = "appName";
    final String operator = "operator";

    final Long val1Count = 1L;
    final Long val1Max = 2L;
    final Long val1Min = 3L;
    final Long val1Sum = 4L;

    final Long val2Count = 1L;
    final Double val2Max = 2.0;
    final Double val2Min = 3.0;
    final Double val2Sum = 4.0;

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

    DataDeserializerFactory qb = new DataDeserializerFactory(CountersSchema.class,
                                                             CountersUpdateDataLogical.class);
    String schemaJSON = test.toString(2);
    logger.debug("schema json: {}", schemaJSON);
    CountersSchema dq = (CountersSchema) qb.deserialize(schemaJSON);

    CountersSchemaUtils.addSchema(dq);

    final String appId = "100";
    final Long windowId = 1L;
    final Long time = 100000000L;
    JSONObject logicalData = new JSONObject();
    logicalData.put(Data.FIELD_TYPE, CountersUpdateDataLogical.TYPE);
    logicalData.put(CountersUpdateDataLogical.FIELD_APP_ID, appId);
    logicalData.put(CountersUpdateDataLogical.FIELD_APP_NAME, appName);
    logicalData.put(CountersUpdateDataLogical.FIELD_USER, user);
    logicalData.put(CountersUpdateDataLogical.FIELD_VERSION, version);
    logicalData.put(CountersUpdateDataLogical.FIELD_LOGICAL_OPERATOR_NAME, operator);
    logicalData.put(CountersUpdateDataLogical.FIELD_WINDOW_ID, windowId);
    logicalData.put(CountersUpdateDataLogical.FIELD_TIME, time);
    JSONObject logicalCountersObj = new JSONObject();

    JSONArray aggVals1 = new JSONArray();
    aggVals1.put(val1Count);
    aggVals1.put(val1Max);
    aggVals1.put(val1Min);
    aggVals1.put(val1Sum);
    logicalCountersObj.put(value1, aggVals1);

    JSONArray aggVals2 = new JSONArray();
    aggVals2.put(val2Count);
    aggVals2.put(val2Max);
    aggVals2.put(val2Min);
    aggVals2.put(val2Sum);
    logicalCountersObj.put(value2, aggVals2);

    logicalData.put(CountersUpdateDataLogical.FIELD_LOGICAL_COUNTERS, logicalCountersObj);
    String logicString = logicalData.toString(2);
    logger.debug("data: {}", logicString);

    CountersUpdateDataLogical cudl = (CountersUpdateDataLogical) qb.deserialize(logicString);

    Assert.assertEquals("The types should equal.", cudl.getType(), CountersUpdateDataLogical.TYPE);
    Assert.assertEquals("The appIds should equal.", appId, cudl.getAppID());
    Assert.assertEquals("The appName should equal.", appName, cudl.getAppName());
    Assert.assertEquals("The user name should equal.", user, cudl.getUser());
    Assert.assertEquals("The version should equal.", version, cudl.getVersion());
    Assert.assertEquals("The logicalOperator should equal.", operator, cudl.getLogicalOperatorName());
    Assert.assertEquals("The windowId should equal.", windowId, cudl.getWindowID());
    Assert.assertEquals("The time should equal.", time, cudl.getTime());

    Map<String, GPOImmutable> vals = cudl.getAggregateToVals();

    GPOImmutable counts = vals.get(AggregatorType.COUNT.getName());
    Assert.assertEquals("The val1 counts should match.", val1Count, counts.getField(value1));
    Assert.assertEquals("The val2 counts should match.", val2Count, counts.getField(value2));

    GPOImmutable max = vals.get(AggregatorType.MAX.getName());
    Assert.assertEquals("The val1 maxes should match.", val1Max, max.getField(value1));
    Assert.assertEquals("The val2 maxes should match.", val2Max, max.getField(value2));

    GPOImmutable min = vals.get(AggregatorType.MIN.getName());
    Assert.assertEquals("The val1 mins should match.", val1Min, min.getField(value1));
    Assert.assertEquals("The val2 mins should match.", val2Min, min.getField(value2));

    GPOImmutable sum = vals.get(AggregatorType.SUM.getName());
    Assert.assertEquals("The val1 sums should match.", val1Sum, sum.getField(value1));
    Assert.assertEquals("The val2 sums should match.", val2Sum, sum.getField(value2));
  }
}
