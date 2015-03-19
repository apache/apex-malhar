/*
 *  Copyright (c) 2012-2015 Malhar, Inc.
 *  All Rights Reserved.
 */

package com.datatorrent.lib.appdata.qr.processor;

import com.datatorrent.lib.appdata.qr.processor.QueueList.QueueListNode;
import org.apache.commons.lang3.mutable.MutableLong;

/**
 *
 * @author Timothy Farkas: tim@datatorrent.com
 */
public class WWEQueryQueueManager<QUERY_TYPE, META_QUERY> extends AbstractWEQueryQueueManager<QUERY_TYPE, META_QUERY, MutableLong>
{
  public WWEQueryQueueManager()
  {
  }

  @Override
  public boolean removeBundle(QueryBundle<QUERY_TYPE, META_QUERY, MutableLong> queryQueueable)
  {
    return queryQueueable.getQueueContext().longValue() <= 0L;
  }

  @Override
  public void endWindow()
  {
    for(QueueListNode<QueryBundle<QUERY_TYPE, META_QUERY, MutableLong>> tempNode = queryQueue.getHead();
        tempNode != null;
        tempNode = tempNode.getNext())
    {
      MutableLong qc = tempNode.getPayload().getQueueContext();
      qc.decrement();
    }
  }
}
