/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.schemas.ads;

import com.datatorrent.lib.appdata.qr.QRType;
import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.ResultSerializerInfo;
import com.datatorrent.lib.appdata.qr.SimpleResultSerializer;


/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */

@QRType(type=AdsUpdateResult.TYPE)
@ResultSerializerInfo(clazz=SimpleResultSerializer.class)
public class AdsUpdateResult extends AdsOneTimeResult
{
  public static final String TYPE = "updateData";

  private Long countdown;

  public AdsUpdateResult(Query query)
  {
    super(query);
  }

  /**
   * @return the countdown
   */
  public Long getCountdown()
  {
    return countdown;
  }

  /**
   * @param countdown the countdown to set
   */
  public void setCountdown(Long countdown)
  {
    this.countdown = countdown;
  }
}
