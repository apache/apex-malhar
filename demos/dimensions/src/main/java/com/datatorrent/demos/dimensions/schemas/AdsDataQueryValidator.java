/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.demos.dimensions.schemas;

import com.datatorrent.demos.dimensions.schemas.AdsDataQuery.AdsDataQueryData;
import com.datatorrent.lib.appdata.qr.CustomDataValidator;
import com.datatorrent.lib.appdata.qr.Data;
import com.datatorrent.lib.appdata.qr.SimpleDataValidator;
import com.datatorrent.lib.appdata.schemas.SchemaUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */

public class AdsDataQueryValidator implements CustomDataValidator
{
  private static final Logger logger = LoggerFactory.getLogger(AdsDataQueryValidator.class);
  private SimpleDataValidator sqv = new SimpleDataValidator();

  public AdsDataQueryValidator()
  {
  }

  @Override
  public boolean validate(Data query)
  {
    if(!(query instanceof AdsDataQuery)) {
      return false;
    }

    if(!sqv.validate(query)) {
      return false;
    }

    AdsDataQuery adq = (AdsDataQuery) query;
    AdsDataQueryData data = adq.getData();

    AdsKeys adsKeys = data.getKeys();
    String advertiser = adsKeys.getAdvertiser();

    if(advertiser != null &&
       !AdsSchemaResult.ADVERTISERS_SET.contains(advertiser)) {
      logger.error("{} is not a valid advertiser.", advertiser);
      return false;
    }

    String publisher = adsKeys.getPublisher();

    if(publisher != null &&
       !AdsSchemaResult.PUBLISHERS_SET.contains(publisher)) {
      logger.error("{} is not a valid publisher.", publisher);
      return false;
    }

    String location = adsKeys.getLocation();

    if(location != null &&
       !AdsSchemaResult.LOCATIONS_SET.contains(location)) {
      logger.error("{} is not a valid location.", location);
      return false;
    }

    AdsTimeRangeBucket atrb = data.getTime();

    if(atrb == null) {
      logger.error("The time section cannot be null.");
      return false;
    }

    if(atrb.getBucket() == null) {
      logger.error("The bucket must be specified.");
      return false;
    }

    if(!AdsSchemaResult.BUCKETS_SET.contains(atrb.getBucket())) {
      logger.error("The value {} is not a valid bucket.", atrb.getBucket());
      return false;
    }

    if((atrb.getFrom() == null) ^
       (atrb.getTo() == null))
    {
      logger.error("Either both from and to must be defined in the query or neither.");
      return false;
    }

    if(atrb.getFrom() != null &&
       atrb.getLatestNumBuckets() != null) {
      logger.error("Cannot define both from and to and latest num buckets.");
      return false;
    }

    if(atrb.getFrom() != null) {
      if(!SchemaUtils.checkDate(atrb.getFrom())) {
        logger.error("Invalid from date: {}", atrb.getFrom());
        return false;
      }

      if(!SchemaUtils.checkDate(atrb.getTo())) {
        logger.error("Invalid to date: {}", atrb.getTo());
        return false;
      }
    }

    return true;
  }
}
