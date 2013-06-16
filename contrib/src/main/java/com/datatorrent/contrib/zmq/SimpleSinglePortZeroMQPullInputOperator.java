/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.zmq;

import com.datatorrent.lib.io.SimpleSinglePortInputOperator;
import com.malhartech.api.Context;
import org.zeromq.ZMQ;

/**
 *
 * @author davidyan
 */
public abstract class SimpleSinglePortZeroMQPullInputOperator<T> extends SimpleSinglePortInputOperator<T> implements Runnable
{
  private transient ZMQ.Context context;
  private transient ZMQ.Socket sock;
  private String zmqAddress = "tcp://127.0.0.1:5555";

  @SuppressWarnings("unused")
  private SimpleSinglePortZeroMQPullInputOperator()
  {
    super();
  }

  public SimpleSinglePortZeroMQPullInputOperator(String addr)
  {
    super();
    zmqAddress = addr;
  }

  @Override
  public void run()
  {
    while (true) {
      byte[] buf = sock.recv(0);
      if (buf == null) {
        continue;
      }
      outputPort.emit(convertFromBytesToTuple(buf));
    }
  }

  protected abstract T convertFromBytesToTuple(byte[] bytes);

  @Override
  public void setup(Context.OperatorContext ctx)
  {
    context = ZMQ.context(1);
    sock = context.socket(ZMQ.PULL);
    sock.connect(zmqAddress);
  }

  @Override
  public void teardown()
  {
    sock.close();
    context.term();
  }

}
