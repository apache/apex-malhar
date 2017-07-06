package org.apache.apex.malhar.python.operator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.PythonConstants;
import org.apache.apex.malhar.lib.window.impl.KeyedWindowedOperatorImpl;
import org.apache.apex.malhar.python.runtime.PythonServer;
import org.apache.apex.malhar.python.operator.proxy.PythonWorkerProxy;

import com.datatorrent.api.Context;

/**
 * Created by vikram on 28/6/17.
 */
public class PythonKeyedWindowedOperator extends KeyedWindowedOperatorImpl
{

  private static final Logger LOG = LoggerFactory.getLogger(PythonWindowedOperator.class);
  private PythonServer server = null;
  protected byte[] serializedFunction = null;
  protected transient PythonConstants.OpType operationType = null;


  public PythonKeyedWindowedOperator()
  {
    this.serializedFunction = null;
  }
  public PythonKeyedWindowedOperator(byte[] serializedFunc)
  {
    this.serializedFunction = serializedFunc;
    this.server = new PythonServer(this.operationType, serializedFunc);
  }

  public void setup(Context.OperatorContext context)
  {
    super.setup(context);
    server.setOperationType(((PythonWorkerProxy)this.accumulation).getOperationType());
    server.setProxy((PythonWorkerProxy)this.accumulation);
    server.setup();
  }

  public void teardown()
  {
    if (server != null) {
      server.shutdown();
    }
  }
}
