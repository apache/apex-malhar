
package com.datatorrent.contrib.accumulo;

import org.apache.accumulo.core.data.Mutation;


public class AccumuloOutputOperator extends AbstractAccumuloOutputOperator<Object>
{

  @Override
  public Mutation operationMutation(Object t)
  {
    return null;
  }

}
