package com.datatorrent.contrib.mongodb;

import javax.annotation.Nonnull;

import com.datatorrent.lib.database.AbstractDBLookupCacheBackedOperator;
import com.datatorrent.lib.database.DBConnector;

/**
 * <br>This is {@link AbstractDBLookupCacheBackedOperator} which retrieves value of a key
 * from MongoDB</br>
 *
 * @param <T> type of input tuples </T>
 * @since 0.9.1
 */
public abstract class MongoDBLookupCacheBackedOperator<T> extends AbstractDBLookupCacheBackedOperator<T>
{
  private final MongoDBOperatorBase mongoDbConnector;

  public MongoDBLookupCacheBackedOperator()
  {
    super();
    mongoDbConnector = new MongoDBOperatorBase();
  }

  @Nonnull
  @Override
  public DBConnector getDbConnector()
  {
    return mongoDbConnector;
  }

  /**
   * Sets the username which is used to connect to mongoDB.
   *
   * @param userName user name
   */
  public void setUserName(String userName)
  {
    mongoDbConnector.setUserName(userName);
  }

  /**
   * Sets the password of mongoDB user.
   *
   * @param passWord password
   */
  public void setPassWord(String passWord)
  {
    mongoDbConnector.setPassWord(passWord);
  }

  /**
   * Sets the database which will be used.
   *
   * @param dataBase database name
   */
  public void setDataBase(String dataBase)
  {
    mongoDbConnector.setDataBase(dataBase);
  }

  /**
   * Sets the host of mongoDB.
   *
   * @param hostName host name of mongo db
   */
  public void setHostName(String hostName)
  {
    mongoDbConnector.setHostName(hostName);
  }
}
