package com.datatorrent.contrib.couchdb;

/**
 * Interface that is implemented by CounchDb Input and Output adaptors.</br>
 *
 * @since 0.3.5
 */
public interface CouchDbOperator
{
  /**
   * Sets the database connection url
   * @param url database connection url
   */
  void setUrl(String url);

  /**
   * Sets the database name
   * @param dbName  database name
   */
  void setDatabase(String dbName);

  /**
   * Sets the database user
   * @param userName database user
   */
  void setUserName(String userName);

  /**
   * Sets the password of the database user
   * @param password password of the database user
   */
  void setPassword(String password);
}
