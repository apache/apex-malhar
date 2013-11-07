package com.datatorrent.contrib.couchdb;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.google.common.base.Preconditions;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.node.ObjectNode;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;

import javax.annotation.Nonnull;

/**
 * <br>Base class for CouchDb intput adaptor.</br>
 * <br>CouchDb filters documents in the database using stored views. Views are refered as design documents.
 * This operator queries the view and emits the view result.</br>
 *
 * <br>Subclasses  of this operator provide the ViewQuery which corresponds to a database view.</br>
 * <br>In this base implementaion, if the ViewQuery doesn't change, then the same view results are emitted
 * at the end of every streaming window.</br>
 *
 * @param <T>Type of tuples which are generated</T>
 * @since 0.3.5
 */
public abstract class AbstractCouchDBInputOperator<T> extends BaseOperator implements CouchDbOperator, InputOperator
{

  private String url;
  @Nonnull
  private String dbName;
  private String userName;
  private String password;

  protected transient CouchDBLink dbLink;
  protected transient ObjectMapper mapper;

  @OutputPortFieldAnnotation(name = "outputPort")
  public final transient DefaultOutputPort<T> outputPort = new DefaultOutputPort<T>();

  @Override
  public void emitTuples()
  {
    ViewQuery viewQuery = getViewQuery();
    ViewResult result = dbLink.getConnector().queryView(viewQuery);
    for (ViewResult.Row row : result.getRows()) {
      JsonNode node = row.getValueAsNode();
      T tuple = getTuple(node);
      outputPort.emit(tuple);
    }
  }

  @Override
  public void setUrl(String url)
  {
    this.url = url;
  }

  @Override
  public void setDatabase(String dbName)
  {
    this.dbName = Preconditions.checkNotNull(dbName, "database");
  }

  @Override
  public void setUserName(String userName)
  {
    this.userName = userName;
  }

  @Override
  public void setPassword(String password)
  {
    this.password = password;
  }

  @Override
  public void setup(Context.OperatorContext context)
  {
    this.dbLink = new CouchDBLink(url, userName, password, dbName);
    this.mapper = new ObjectMapper();
  }

  /**
   *
   * @return  view-query that specifies the couch-db view whose results will be fetched.
   */
  public abstract ViewQuery getViewQuery();

  /**
   * This operator fetches view result in form of {@link JsonNode}. Sub-classes should provie the
   * implementaion to convert the jsonNode object to emitted tuple type.
   *
   * @param value jsonNode that will converted to a tuple.
   * @return emmitted tuple.
   */
  public abstract T getTuple(JsonNode value);

}
