/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.lib.appdata.qr.Query;
import com.datatorrent.lib.appdata.qr.processor.QueueList.QueueListNode;
import com.google.common.collect.Maps;
import org.apache.commons.lang3.mutable.MutableLong;

import java.util.Map;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class AppDataWWEQueryQueueManager<QUERY extends Query, META_QUERY> extends WWEQueryQueueManager<QUERY, META_QUERY>
{
  private Map<String, QueueListNode<QueryBundle<QUERY, META_QUERY, MutableLong>>> queryIDToNode = Maps.newHashMap();

  public AppDataWWEQueryQueueManager()
  {
  }

  @Override
  public void addedNode(QueueListNode<QueryBundle<QUERY, META_QUERY, MutableLong>> queryQueueable)
  {
    queryIDToNode.put(queryQueueable.getPayload().getQuery().getId(), queryQueueable);
  }

  @Override
  public void removedNode(QueueListNode<QueryBundle<QUERY, META_QUERY, MutableLong>> queryQueueable)
  {
    queryIDToNode.remove(queryQueueable.getPayload().getQuery().getId());
  }

  @Override
  public boolean addingFilter(QueryBundle<QUERY, META_QUERY, MutableLong> queryBundle)
  {
    QueueListNode<QueryBundle<QUERY, META_QUERY, MutableLong>> queryNode =
    queryIDToNode.get(queryBundle.getQuery().getId());

    if(queryNode == null) {
      return true;
    }

    queryNode.setPayload(queryBundle);
    return false;
  }
}
