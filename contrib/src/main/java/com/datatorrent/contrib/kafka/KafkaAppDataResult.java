/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.kafka;

import com.datatorrent.api.AppDataOperator;
import java.net.URI;
import java.net.URISyntaxException;
import org.apache.http.client.utils.URIBuilder;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class KafkaAppDataResult extends KafkaSinglePortOutputOperator<String, String> implements AppDataOperator
{
  @Override
  public URI getAppDataURL()
  {
    String brokerList = this.getConfigProperties().getProperty("metadata.broker.list");
    String[] brokers = brokerList.split(",");
    String mainBroker = brokers[0];
    String[] splitMain = mainBroker.split(":");

    URIBuilder ub = new URIBuilder();
    ub.setScheme("kafka");
    ub.setHost(splitMain[0]);
    ub.setPort(Integer.parseInt(splitMain[1]));
    ub.addParameter("brokerSet", brokerList);
    ub.addParameter("topic", getTopic());

    URI uri = null;

    try {
      uri = ub.build();
    }
    catch(URISyntaxException ex) {
      throw new RuntimeException(ex);
    }

    return uri;
  }
}
