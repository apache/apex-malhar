/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.apex.malhar.contrib.r;

import java.io.IOException;

import org.rosuda.REngine.REngine;
import org.rosuda.REngine.REngineCallbacks;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.apex.malhar.lib.db.Connectable;

import com.datatorrent.netlet.util.DTThrowable;

/**
 * @since 2.1.0
 */
public class REngineConnectable implements Connectable
{

  private transient REngine rengine;
  private static Logger log = LoggerFactory.getLogger(REngineConnectable.class);

  // REngine Parameters
  String rEngineClassName = "org.rosuda.REngine.JRI.JRIEngine";

  // Parameters to R while creating workspace. Default as
  // --vanilla => Combine --no-save, --no-restore, --no-site-file, --no-init-file and --no-environ
  // Read man page of R for details.
  String[] args = {"--vanilla"};

  // No callbacks used, can use REngineStdOutput
  // (http://www.rforge.net/org/doc/org/rosuda/REngine/REngineStdOutput.html)
  // which is the only callback implemented so far.
  REngineCallbacks callBacks = null;

  // Wiki: A read–eval–print loop (REPL), also known as an interactive toplevel or language shell,
  // setting to false as not using R in interactive mode.
  boolean runREPL = false;

  /*
   * (non-Javadoc)
   *
   * @see org.apache.apex.malhar.lib.db.Connectable#connect()
   */
  @Override
  public void connect() throws IOException
  {
    try {

      rengine = REngine.getLastEngine();
      if (rengine == null) {
        // new R-engine
        rengine = REngine.engineForClass(rEngineClassName, args, callBacks, runREPL);
        log.info("Creating new Rengine");
      } else {
        log.info("Got last Rengine");
      }
    } catch (Exception exc) {
      log.error("Exception: ", exc);
      DTThrowable.rethrow(exc);
    }

  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.apex.malhar.lib.db.Connectable#disconnect()
   */
  @Override
  public void disconnect() throws IOException
  {
    if (rengine != null) {
      rengine.close();
    }
  }

  /*
   * (non-Javadoc)
   *
   * @see org.apache.apex.malhar.lib.db.Connectable#isConnected()
   */
  @Override
  public boolean isConnected()
  {
    return rengine == null ? false : true;
  }

  /**
   * @return the rengine
   */
  public REngine getRengine()
  {
    return rengine;
  }

  /**
   * @return the rEngineClassName
   */
  public String getrEngineClassName()
  {
    return rEngineClassName;
  }

  /**
   * @param rEngineClassName
   *          the rEngineClassName to set
   */
  public void setrEngineClassName(String rEngineClassName)
  {
    this.rEngineClassName = rEngineClassName;
  }

  /**
   * @return the args
   */
  public String[] getArgs()
  {
    return args;
  }

  /**
   * @param args
   *          the args to set
   */
  public void setArgs(String[] args)
  {
    this.args = args;
  }

  /**
   * @return the callBacks
   */
  public REngineCallbacks getCallBacks()
  {
    return callBacks;
  }

  /**
   * @param callBacks
   *          the callBacks to set
   */
  public void setCallBacks(REngineCallbacks callBacks)
  {
    this.callBacks = callBacks;
  }

  /**
   * @return the runREPL
   */
  public boolean isRunREPL()
  {
    return runREPL;
  }

  /**
   * @param runREPL
   *          the runREPL to set
   */
  public void setRunREPL(boolean runREPL)
  {
    this.runREPL = runREPL;
  }

}
