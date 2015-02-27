/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.twitter.schemas;

import org.codehaus.jackson.map.annotate.JsonSerialize;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class TwitterDataValues
{
  @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
  private String url;
  @JsonSerialize(include=JsonSerialize.Inclusion.NON_NULL)
  private Integer count;

  public TwitterDataValues()
  {
  }

  /**
   * @return the url
   */
  public String getUrl()
  {
    return url;
  }

  /**
   * @param url the url to set
   */
  public void setUrl(String url)
  {
    this.url = url;
  }

  /**
   * @return the count
   */
  public Integer getCount()
  {
    return count;
  }

  /**
   * @param count the count to set
   */
  public void setCount(Integer count)
  {
    this.count = count;
  }
}
