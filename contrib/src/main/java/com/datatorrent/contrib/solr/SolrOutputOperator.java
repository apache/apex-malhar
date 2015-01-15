package com.datatorrent.contrib.solr;

import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.solr.common.SolrInputDocument;

/**
 * Default Implementation of AbstractSolrOutputOperator. Accepts maps and puts key values in SolrInputDocument format.
 * Map keys must be added to schema.xml <br>
 */
public class SolrOutputOperator extends AbstractSolrOutputOperator<Map<String, Object>, SolrServerConnector>
{

  @Override
  public SolrInputDocument convertTuple(Map<String, Object> tupleFields)
  {
    SolrInputDocument inputDoc = new SolrInputDocument();
    Iterator<Entry<String, Object>> itr = tupleFields.entrySet().iterator();
    while (itr.hasNext()) {
      Entry<String, Object> field = itr.next();
      inputDoc.setField(field.getKey(), field.getValue());
    }
    return inputDoc;
  }

}
