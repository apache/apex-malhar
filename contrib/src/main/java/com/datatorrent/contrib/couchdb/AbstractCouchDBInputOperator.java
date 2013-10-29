package com.datatorrent.contrib.couchdb;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.google.common.base.Preconditions;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.ektorp.ViewQuery;
import org.ektorp.ViewResult;

import javax.annotation.Nonnull;

/**
 * <br>Base class for CouchDb intput adaptor.</br>
 * <br>It emits the result of ViewQuery which is implemented by the base adaptors.</br>
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
      T tuple = getTuple(row.getId(), node);
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

  public abstract ViewQuery getViewQuery();

  public abstract T getTuple(String id, JsonNode value);

}
