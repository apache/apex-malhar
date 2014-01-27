package com.datatorrent.lib.db;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

/**
 * A {@link Connectable} for Document based databases.
 *
 * @param <T> type of document </T>
 */
public interface DocumentStore<T> extends Connectable
{
  /**
   * Inserts document in the document store.
   *
   * @param docId    document Id.
   * @param document document to be inserted.
   */
  void insertDocument(@Nonnull String docId, @Nonnull T document);

  /**
   * Update or insert a document in the store.
   *
   * @param docId    document Id.
   * @param document document to be updated or inserted.
   */
  void upsertDocument(@Nonnull String docId, @Nonnull T document);

  /**
   * Finds if a document is present in the database or not.
   *
   * @param docId document Id.
   * @return true if document is in the database; false otherwise.
   */
  boolean containsDocument(String docId);

  /**
   * Fetches a document identified by the doc id from the database.
   *
   * @param docId document Id.
   * @return document in the database.
   */
  @Nullable
  T getDocument(String docId);

}
