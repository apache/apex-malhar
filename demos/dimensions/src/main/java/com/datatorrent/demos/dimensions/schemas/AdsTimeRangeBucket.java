/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import javax.validation.constraints.NotNull;

import java.util.Date;

import org.codehaus.jackson.annotate.JsonIgnore;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AdsTimeRangeBucket
{
  public static final SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss:SSS");

  @NotNull
  private String from;
  @NotNull
  private String to;
  @NotNull
  private String bucket;

  private int latestNumBuckets;

  public AdsTimeRangeBucket()
  {
  }

  /**
   * @return the from
   */
  public String getFrom()
  {
    return from;
  }

  /**
   * @param from the from to set
   */
  public void setFrom(String from)
  {
    this.from = from;
  }

  @JsonIgnore
  public long getFromLong()
  {
    try {
      return sdf.parse(this.getFrom()).getTime();
    }
    catch(ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  @JsonIgnore
  public void setFromLong(long from)
  {
    this.setFrom(sdf.format(new Date(from)));
  }

  /**
   * @return the to
   */
  public String getTo()
  {
    return to;
  }

  /**
   * @param to the to to set
   */
  public void setTo(String to)
  {
    this.to = to;
  }

  @JsonIgnore
  public long getToLong()
  {
    try {
      return sdf.parse(this.getTo()).getTime();
    }
    catch(ParseException ex) {
      throw new RuntimeException(ex);
    }
  }

  @JsonIgnore
  public void setToLong(long to)
  {
    this.setTo(sdf.format(new Date(to)));
  }

  /**
   * @return the bucket
   */
  public String getBucket()
  {
    return bucket;
  }

  /**
   * @param bucket the bucket to set
   */
  public void setBucket(String bucket)
  {
    this.bucket = bucket;
  }

  /**
   * @return the latestNumBuckets
   */
  public int getLatestNumBuckets()
  {
    return latestNumBuckets;
  }

  /**
   * @param latestNumBuckets the latestNumBuckets to set
   */
  public void setLatestNumBuckets(int latestNumBuckets)
  {
    this.latestNumBuckets = latestNumBuckets;
  }
}
