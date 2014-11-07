package com.datatorrent.contrib.couchbase;

/**
 * Created by Pramod Immaneni <pramod@datatorrent.com> on 11/3/14.
 */
public interface CouchBaseSerializer
{
  public Object serialize(Object o);
}
