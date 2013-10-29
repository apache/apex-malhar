package com.datatorrent.contrib.couchdb;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * CouchDb tuple type that can be persisted by {@link AbstractCouchDBOutputOperator}
 */
public interface CouchDbTuple
{

  @Nullable
  public String getId();

  @Nullable
  public String getRevision();


  /**
   * @return object that is compatible with {@link org.ektorp.CouchDbConnector}
   */
  @Nonnull
  public Object getPayLoad();

  public void setRevision(String revision);

  public void setId(String id);
}
