package com.datatorrent.contrib.couchdb;

/**
 * Interface that is implemented by CounchDb Input and Output adaptors.<br></br>
 *
 * @since 0.3.5
 */
public interface CouchDbOperator
{

  void setUrl(String url);

  void setDatabase(String dbName);

  void setUserName(String userName);

  void setPassword(String password);
}
