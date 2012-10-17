/*
 *  Copyright (c) 2012 Malhar, Inc.
 *  All Rights Reserved.
 */
package com.malhartech.demos.twitter;

import com.malhartech.annotation.ModuleAnnotation;
import com.malhartech.annotation.PortAnnotation;
import com.malhartech.dag.AbstractModule;
import com.malhartech.dag.Component;
import com.malhartech.dag.Sink;
import twitter4j.Status;
import twitter4j.URLEntity;

/**
 *
 * @author Chetan Narsude <chetan@malhar-inc.com>
 */
@ModuleAnnotation(ports = {
  @PortAnnotation(name = Component.INPUT, type = PortAnnotation.PortType.INPUT),
  @PortAnnotation(name = Component.OUTPUT, type = PortAnnotation.PortType.OUTPUT)
})
public class TwitterStatusURLExtractor extends AbstractModule implements Sink<Status>
{
  @Override
  public void process(Status status)
  {
    URLEntity[] entities = status.getURLEntities();
    if (entities != null) {
      for (URLEntity ue: entities) {
        emit((ue.getExpandedURL() == null ? ue.getURL() : ue.getExpandedURL()).toString());
      }
    }
  }
}
