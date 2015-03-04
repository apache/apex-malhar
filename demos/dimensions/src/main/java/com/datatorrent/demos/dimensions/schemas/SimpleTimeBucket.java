/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class SimpleTimeBucket
{
  private String bucket;

  public SimpleTimeBucket()
  {
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
}
