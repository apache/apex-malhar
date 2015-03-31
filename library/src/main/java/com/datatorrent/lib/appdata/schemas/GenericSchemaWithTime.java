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

    boolean fromSpecified;

    try {
      from = time.getString(FIELD_TIME_FROM);
      SchemaUtils.checkDateEx(from);
      fromSpecified = true;
    }
    catch(JSONException ex) {
      fromSpecified = false;
    }

    boolean toSpecified;

    try {
      to = time.getString(FIELD_TIME_TO);
      SchemaUtils.checkDateEx(to);
      toSpecified = true;
    }
    catch(JSONException ex) {
      toSpecified = false;
    }

    JSONArray bucketArray = time.getJSONArray(FIELD_TIME_BUCKETS);

    Preconditions.checkState(!(fromSpecified ^ toSpecified),
                             "Either both the from and to fields must be specified or none.");

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

  public String getTo()
  {
    return to;
  }

  public Set<TimeBucket> getBuckets()
  {
    return buckets;
  }
}
