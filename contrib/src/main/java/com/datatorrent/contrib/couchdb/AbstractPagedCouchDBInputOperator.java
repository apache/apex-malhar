package com.datatorrent.contrib.couchdb;

import org.codehaus.jackson.JsonNode;
import org.ektorp.Page;
import org.ektorp.PageRequest;

import javax.validation.constraints.Min;

/**
 * <br>This operator emits paged results. The page size is configured using operator property.</br>
 * <br>The operator assumes that the ViewQuery implementation by sub-classes does not depend on data oustide the document
 * (like the current date) because that will break the caching of a view's result in CouchDb.
 * Also the getViewQuery() method should return the same view stored in CouchDb everytime. </br>
 *
 * @since 0.3.5
 */
public abstract class AbstractPagedCouchDBInputOperator<T> extends AbstractCouchDBInputOperator<T>
{
  @Min(0)
  private int pageSize;

  private String nextPageLink = null;
  private boolean started = false;

  public AbstractPagedCouchDBInputOperator()
  {
    super();
    pageSize = 0;
  }

  public void setPageSize(int pageSize)
  {
    this.pageSize = pageSize;
  }

  @Override
  public void emitTuples()
  {
    if (pageSize == 0)     //No pagination
      super.emitTuples();
    else {
      PageRequest pageRequest = null;
      if (!started) {
        started = true;
        pageRequest = PageRequest.firstPage(pageSize);
      }
      else if (nextPageLink != null) {
        pageRequest = PageRequest.fromLink(nextPageLink);
      }
      if (pageRequest != null) {
        Page<JsonNode> result = dbLink.getConnector().queryForPage(getViewQuery(), pageRequest, JsonNode.class);

        for (JsonNode node : result) {
          T tuple = getTuple(node);
          outputPort.emit(tuple);
        }
        if (result.isHasNext())
          nextPageLink = result.getNextLink();
        else
          nextPageLink = null;
      }
    }
  }
}
