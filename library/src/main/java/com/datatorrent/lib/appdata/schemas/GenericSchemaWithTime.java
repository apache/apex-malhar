/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas;

import com.google.common.base.Preconditions;
import com.google.common.collect.Sets;
import java.io.InputStream;
import org.codehaus.jettison.json.JSONArray;
import org.codehaus.jettison.json.JSONException;
import org.codehaus.jettison.json.JSONObject;

import java.util.Collections;
import java.util.Set;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class GenericSchemaWithTime extends GenericSchemaTabular
{
  public static final int NUM_KEYS_TIME = 3;
  public static final int NUM_KEYS_TIME_NO_FROM_AND_TO = 1;

  public static final String FIELD_TIME = "time";
  public static final String FIELD_TIME_FROM = "from";
  public static final String FIELD_TIME_TO = "to";
  public static final String FIELD_TIME_BUCKETS = "buckets";

  private String from;
  private String to;
  private Set<TimeBucket> buckets;
  private boolean fromTo;

  GenericSchemaWithTime(InputStream inputStream)
  {
    this(SchemaUtils.inputStreamToString(inputStream));
  }

  GenericSchemaWithTime(String schemaJSON)
  {
    super(schemaJSON, false);

    try {
      initialize();
    }
    catch(Exception ex) {
      throw new RuntimeException(ex);
    }
  }

  private void initialize() throws Exception
  {
    JSONObject schema = new JSONObject(getSchemaJSON());
    JSONObject time = schema.getJSONObject(FIELD_TIME);

    boolean fromSpecified = time.has(FIELD_TIME_FROM);

    if(fromSpecified) {
      from = time.getString(FIELD_TIME_FROM);
      SchemaUtils.checkDateEx(from);
    }

    boolean toSpecified = time.has(FIELD_TIME_TO);

    if(toSpecified) {
      to = time.getString(FIELD_TIME_TO);
      SchemaUtils.checkDateEx(to);
    }

    Preconditions.checkState(!(fromSpecified ^ toSpecified),
                             "Either both the from and to fields must be specified or none.");

    fromTo = fromSpecified;

    JSONArray bucketArray = time.getJSONArray(FIELD_TIME_BUCKETS);

    if(fromSpecified && toSpecified) {
      Preconditions.checkState(time.length() == NUM_KEYS_TIME,
                             "Expected " + NUM_KEYS_TIME +
                             " keys under " + FIELD_TIME +
                             ", but found " + time.length());

    }
    else
    {
      Preconditions.checkState(time.length() == NUM_KEYS_TIME_NO_FROM_AND_TO,
                             "Expected " + NUM_KEYS_TIME_NO_FROM_AND_TO +
                             " keys under " + FIELD_TIME +
                             ", but found " + time.length());
    }

    Preconditions.checkState(bucketArray.length() > 0,
                             FIELD_TIME_BUCKETS + " should not be empty.");

    this.buckets = Sets.newHashSet();

    for(int index = 0;
        index < bucketArray.length();
        index++)
    {
      String bucket = bucketArray.getString(index);
      TimeBucket timeBucket = TimeBucket.getBucket(bucket);

      Preconditions.checkArgument(buckets.add(timeBucket),
                                  "Cannot specify the TimeBucket " +
                                  bucket + " multiple times.");
    }

    this.buckets = Collections.unmodifiableSet(buckets);
  }

  public String getFrom()
  {
    return from;
  }

  public void setFrom(long time)
  {
    String dateString = getDateString(time);

    try {
      JSONObject schemaJo = new JSONObject(getSchemaJSON());
      JSONObject timeJo = schemaJo.getJSONObject(FIELD_TIME);

      timeJo.put(FIELD_TIME_FROM, dateString);

      this.setSchema(schemaJo.toString());
    }
    catch(JSONException ex) {
      throw new RuntimeException(ex);
    }
  }

  public void setTo(long time)
  {
    String dateString = getDateString(time);

    try {
      JSONObject schemaJo = new JSONObject(getSchemaJSON());
      JSONObject timeJo = schemaJo.getJSONObject(FIELD_TIME);

      timeJo.put(FIELD_TIME_TO, dateString);

      this.setSchema(schemaJo.toString());
    }
    catch(JSONException ex) {
      throw new RuntimeException(ex);
    }
  }

  private String getDateString(long time) {
    if(!fromTo) {
      Preconditions.checkState(fromTo, "This schema does not have from or to specified.");
    }

    return SchemaUtils.getDateString(time);
  }

  public String getTo()
  {
    return to;
  }

  public Set<TimeBucket> getBuckets()
  {
    return buckets;
  }

  /**
   * @return the fromTo
   */
  public boolean isFromTo()
  {
    return fromTo;
  }
}
