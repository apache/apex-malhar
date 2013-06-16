/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.mongodb;

import com.mongodb.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import javax.validation.constraints.Min;
import javax.validation.constraints.NotNull;

/**
 * MongoDB base operator, which has basic information for an i/o operator.<p><br>
 *
 * <br>
 * Properties:<br>
 * <b>hostName</b>:the host name of the database to connect to, not null<br>
 * <b>dataBase</b>:the database to connect to<br>
 * <b>userName</b>:userName for connection to database<br>
 * <b>passWord</b>:password for connection to database<br>
 * <b>mongoClient</b>:created when connected to database<br>
 * <b>db</b>:created when connected to database<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * hostName
 * batchSize <br>
 * <b>data type:</br>the insertion data can support all the Objects mongoDB supports<br>
 *
 * <b>Benchmarks</b>:
 * <br>
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class MongoDBBaseOperator
{
  @NotNull
  protected String hostName;
  protected String dataBase;

  protected String userName;
  protected String passWord;
  protected transient MongoClient mongoClient;
  protected transient DB db;


  public String getUserName()
  {
    return userName;
  }

  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  public String getPassWord()
  {
    return passWord;
  }

  public void setPassWord(String passWord)
  {
    this.passWord = passWord;
  }


  public String getDataBase()
  {
    return dataBase;
  }

  public void setDataBase(String dataBase)
  {
    this.dataBase = dataBase;
  }

  public String getHostName()
  {
    return hostName;
  }

  public void setHostName(String dbUrl)
  {
    this.hostName = dbUrl;
  }

}
