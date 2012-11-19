/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.kestrel;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kestrel output adapter operator, which produce data to Kestrel message bus.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>queueName</b>:the queueName to interact with kestrel server<br>
 * <b>servers</b>:the kestrel server url list<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>: Blast as many tuples as possible in inline mode<br>
 * Operator can process about 1 thousand unique (k,v immutable pairs) tuples/sec as Kestrel DAG. The performance is directly proportional to key,val pairs emitted<br>
 * <br>
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class AbstractKestrelOutputOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractKestrelOutputOperator.class);
  public String queueName;
  String[] servers;
  private transient SockIOPool pool;
  protected transient MemcachedClient mcc;

  @Override
  public void setup(OperatorContext context)
  {
    pool = SockIOPool.getInstance();
    pool.setServers(servers);
    pool.setFailover(true);
    pool.setInitConn(10);
    pool.setMinConn(5);
    pool.setMaxConn(250);
    pool.setMaintSleep(30);
    pool.setNagle(false);
    pool.setSocketTO(3000);
    pool.setAliveCheck(true);
    pool.initialize();

    mcc = new MemcachedClient();
    mcc.flush(queueName, null);
  }

  public void setQueueName(String queueName)
  {
    this.queueName = queueName;
  }

  public void setServers(String[] servers)
  {
    this.servers = servers;
  }

  @Override
  public void teardown()
  {
  }
}
