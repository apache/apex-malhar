/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.mongodb;

import com.malhartech.annotation.OutputPortFieldAnnotation;
import com.malhartech.api.ActivationListener;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.InputOperator;
import com.malhartech.util.CircularBuffer;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.slf4j.LoggerFactory;

/**
 * MongoDB input adapter operator, which send selection data from database.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have one input port <br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>tableList</b>: the list of all the tables of the mapping<br>
 * <b>tableToDocument</b>:each tuple corresponds to one document for one collection to be inserted<br>
 * <b>tableToDocumentList</b>:for bulk insert, each table has a document list to insert. This is table and document list map <br>
 * <b>tupleId</b>:the Id of the tuple, incrementing at each tuple process, start from 1 at beginWindow()<br>
 * <b>queryFunction</b>:corresponding to the option for the ObjectId of 12 bytes format saving. The windowId, tupleId, operatorId of each tuple are saved in each collection as the column ObjectId for recovery<br>
 * It Currently has 3 format for the ObjectId. When the operator recovers, it will remove the document which has the same windowId, operatorId as maxWindowTable in the collections, and insert the documents again<br>
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
public abstract class MongoDBInputOperator<T> extends MongoDBBaseOperator implements InputOperator, ActivationListener<OperatorContext>
{
  private static final org.slf4j.Logger logger = LoggerFactory.getLogger(MongoDBInputOperator.class);
  private String table;
  private DBObject query;
  private transient DBCursor resultCursor;
  @OutputPortFieldAnnotation(name = "outputPort")
  final public transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>(this);

  /**
   * Any concrete class derived from this has to implement this method
   * so that it knows what type of message it is going to send to Malhar.
   * It converts a byte message into a Tuple. A Tuple can be of any type (derived from Java Object) that
   * operator user intends to.
   *
   * @param message
   */
  public abstract T getTuple(DBCursor result);

  @Override
  public void emitTuples()
  {
    resultCursor = db.getCollection(table).find(query);
    outputPort.emit(getTuple(resultCursor));
  }

  @Override
  public void beginWindow(long windowId)
  {
    try {
      mongoClient = new MongoClient(hostName);
      db = mongoClient.getDB(dataBase);
      if (userName != null && passWord != null) {
        db.authenticate(userName, passWord.toCharArray());
      }
    }
    catch (UnknownHostException ex) {
      logger.debug(ex.toString());
    }

  }

  @Override
  public void endWindow()
  {
  }

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void activate(OperatorContext ctx)
  {
  }

  @Override
  public void deactivate()
  {
  }

  public String getTable()
  {
    return table;
  }

  public void setTable(String table)
  {
    this.table = table;
  }

  public DBObject getQuery()
  {
    return query;
  }

  public void setQuery(DBObject query)
  {
    this.query = query;
  }
}
