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

  /**
   * Each document in couch db has a revision. This sets the revision of the corresponding
   * document object in memory.
   *
   * @param revision revision
   */
  public void setRevision(String revision);

  /**
   * Each document in couch db has an id. This sets the id of the corresponding document
   * object in memory.
   *
   * @param id
   */
  public void setId(String id);
}
