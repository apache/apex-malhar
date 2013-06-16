/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.datatorrent.contrib.kestrel;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.Context.OperatorContext;
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
 * <table border="1" cellspacing=1 cellpadding=1 summary="Benchmark table for AbstractKestrelOutputOperator&lt;K,V extends Number&gt; operator template">
 * <tr><th>In-Bound</th><th>Out-bound</th><th>Comments</th></tr>
 * <tr><td>One tuple per key per window per port</td><td><b>10 thousand K,V pairs/s</td><td>Out-bound rate is the main determinant of performance. Operator can process about 1 thousand unique (k,v immutable pairs) tuples/sec as Kestrel DAG. Tuples are assumed to be
 * immutable. If you use mutable tuples and have lots of keys, the benchmarks may differ</td></tr>
 * </table><br>
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
