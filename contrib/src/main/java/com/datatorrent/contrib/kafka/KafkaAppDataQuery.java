/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.contrib.kafka;

import com.datatorrent.api.AppDataOperator;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.Iterator;
import org.apache.http.client.utils.URIBuilder;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class KafkaAppDataQuery extends KafkaSinglePortStringInputOperator implements AppDataOperator
{
  @Override
  public URI getAppDataURL()
  {
    String mainBroker = this.consumer.brokerSet.iterator().next();
    StringBuilder sb = new StringBuilder();

    Iterator<String> bi = this.consumer.brokerSet.iterator();

    while(bi.hasNext()) {
      sb.append(bi.next());

      if(!bi.hasNext()) {
        break;
      }

      sb.append(",");
    }

    URIBuilder ub = new URIBuilder();
    ub.setScheme("kafka");
    ub.setHost(mainBroker);
    ub.addParameter("brokerSet", sb.toString());
    ub.addParameter("topic", this.consumer.getTopic());

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
