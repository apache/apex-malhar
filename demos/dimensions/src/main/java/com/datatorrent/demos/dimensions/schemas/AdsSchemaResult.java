/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.Result;
import com.datatorrent.lib.appdata.qr.ResultSerializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleResultSerializer;
import com.datatorrent.lib.appdata.schemas.SchemaValues;
import com.datatorrent.lib.appdata.schemas.TimeRangeBuckets;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;

import java.util.Arrays;
import java.util.List;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */

@QRType(type=AdsSchemaResult.TYPE)
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class AdsSchemaResult extends Result
{
  public static final String TYPE = "schemaData";
  public static final String SCHEMA_TYPE = "dimensions";
  public static final String SCHEMA_VERSION = "1.0";
  public static final String[] BUCKETS = {"1m", "1h", "1d"};
  public static final String PUBLISHER = "publisher";
  public static final String[] PUBLISHERS = {"twitter", "facebook", "yahoo",
                                             "google", "bing", "amazon"};
  public static final String ADVERTISER = "advertiser";
  public static final String[] ADVERTISERS = {"starbucks", "safeway", "mcdonalds",
                                              "macys", "taco bell", "walmart", "khol's",
                                              "san diego zoo", "pandas", "jack in the box",
                                              "tomatina", "ron swanson"};
  public static final String LOCATION = "location";
  public static final String[] LOCATIONS = {"N", "LREC", "SKY",
                                            "AL", "AK", "AZ",
                                            "AR", "CA", "CO",
                                            "CT", "DE", "FL",
                                            "GA", "HI", "ID"};
  public static final String TIME = "time";
  public static final String IMPRESSIONS = "impressions";
  public static final String IMPRESSIONS_TYPE = "integer";
  public static final String CLICKS = "clicks";
  public static final String CLICKS_TYPE = "integer";
  public static final String COST = "cost";
  public static final String COST_TYPE = "float";
  public static final String REVENUE = "revenue";
  public static final String REVENUE_TYPE = "float";
  public static final String FROM = "2015-01-01 00:00:00";
  public static final String TO = "2025-12-31 23:59:59";

  public static final Set<String> FIELDS = ImmutableSet.of(ADVERTISER, PUBLISHER, LOCATION,
                                                           TIME, IMPRESSIONS, CLICKS,
                                                           COST, REVENUE);

  private AdsSchemaData data;

  public AdsSchemaResult(Query query)
  {
    super(query);
    data = new AdsSchemaData();
    data.setSchemaType(SCHEMA_TYPE);
    data.setSchemaVersion(SCHEMA_VERSION);

    TimeRangeBuckets trb = new TimeRangeBuckets();
    trb.setFrom(FROM);
    trb.setTo(TO);
    trb.setBuckets(Arrays.asList(BUCKETS));

    data.setTimeBuckets(trb);

    List<KeyMultiValue> kmvs = Lists.newArrayList();

    KeyMultiValue kmv = new KeyMultiValue();
    kmv.setName(PUBLISHER);
    kmv.setKeyValues(Arrays.asList(PUBLISHERS));
    kmvs.add(kmv);

    kmv = new KeyMultiValue();
    kmv.setName(ADVERTISER);
    kmv.setKeyValues(Arrays.asList(ADVERTISERS));
    kmvs.add(kmv);

    kmv = new KeyMultiValue();
    kmv.setName(LOCATION);
    kmv.setKeyValues(Arrays.asList(LOCATIONS));
    kmvs.add(kmv);

    data.setKeys(kmvs);

    List<SchemaValues> svs = Lists.newArrayList();
    SchemaValues sv = new SchemaValues();
    sv.setName(IMPRESSIONS);
    sv.setType(IMPRESSIONS_TYPE);
    svs.add(sv);

    sv = new SchemaValues();
    sv.setName(CLICKS);
    sv.setType(CLICKS_TYPE);
    svs.add(sv);

    sv = new SchemaValues();
    sv.setName(COST);
    sv.setType(COST_TYPE);
    svs.add(sv);

    sv = new SchemaValues();
    sv.setName(REVENUE);
    sv.setType(REVENUE_TYPE);
    svs.add(sv);

    data.setValues(svs);
  }

  /**
   * @return the data
   */
  public AdsSchemaData getData()
  {
    return data;
  }

  /**
   * @param data the data to set
   */
  public void setData(AdsSchemaData data)
  {
    this.data = data;
  }

  public static class AdsSchemaData
  {
    private String schemaType;
    private String schemaVersion;

    private TimeRangeBuckets timeBuckets;
    private List<KeyMultiValue> keys;
    private List<SchemaValues> values;

    public AdsSchemaData()
    {
    }

    /**
     * @return the timeBuckets
     */
    public TimeRangeBuckets getTimeBuckets()
    {
      return timeBuckets;
    }

    /**
     * @param timeBuckets the timeBuckets to set
     */
    public void setTimeBuckets(TimeRangeBuckets timeBuckets)
    {
      this.timeBuckets = timeBuckets;
    }

    /**
     * @return the keys
     */
    public List<KeyMultiValue> getKeys()
    {
      return keys;
    }

    /**
     * @param keys the keys to set
     */
    public void setKeys(List<KeyMultiValue> keys)
    {
      this.keys = keys;
    }

    /**
     * @return the values
     */
    public List<SchemaValues> getValues()
    {
      return values;
    }

    /**
     * @param values the values to set
     */
    public void setValues(List<SchemaValues> values)
    {
      this.values = values;
    }

    /**
     * @return the schemaType
     */
    public String getSchemaType()
    {
      return schemaType;
    }

    /**
     * @param schemaType the schemaType to set
     */
    public void setSchemaType(String schemaType)
    {
      this.schemaType = schemaType;
    }

    /**
     * @return the schemaVersion
     */
    public String getSchemaVersion()
    {
      return schemaVersion;
    }

    /**
     * @param schemaVersion the schemaVersion to set
     */
    public void setSchemaVersion(String schemaVersion)
    {
      this.schemaVersion = schemaVersion;
    }
  }
}
