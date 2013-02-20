/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package com.malhartech.contrib.memcache;

import com.malhartech.api.BaseOperator;
import com.malhartech.api.Context.OperatorContext;
import java.io.IOException;
import java.util.ArrayList;
import java.util.logging.Level;
import net.spy.memcached.AddrUtil;
import net.spy.memcached.MemcachedClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Memcache output adapter operator, which produce data to Memcached.<p><br>
 *
 * <br>
 * Ports:<br>
 * <b>Input</b>: Can have any number of input ports<br>
 * <b>Output</b>: no output port<br>
 * <br>
 * Properties:<br>
 * <b>servers</b>:the memcache server url list<br>
 * <b>pool</b>:memcache sockIOPool<br>
 * <b>mcc</b>:MemCachedClient instance<br>
 * <br>
 * Compile time checks:<br>
 * None<br>
 * <br>
 * Run time checks:<br>
 * None<br>
 * <br>
 * <b>Benchmarks</b>:TBD
 * <br>
 *
 * @author Zhongjian Wang <zhongjian@malhar-inc.com>
 */
public class AbstractMemcacheOutputOperator extends BaseOperator
{
  private static final Logger logger = LoggerFactory.getLogger(AbstractMemcacheOutputOperator.class);
  protected transient MemcachedClient client;
  ArrayList<String> servers = new ArrayList<String>();

  @Override
  public void setup(OperatorContext context)
  {
    try {
      client = new MemcachedClient(AddrUtil.getAddresses(servers));
    }
    catch (IOException ex) {
      logger.info(ex.toString());
    }

  }

  public void addServer(String server)
  {
    servers.add(server);
  }

  @Override
  public void teardown()
  {
  }

}
