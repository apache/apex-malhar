/*
 * Copyright (c) 2014 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.datatorrent.contrib.r;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Map;

import javax.management.RuntimeErrorException;
import javax.validation.constraints.NotNull;

import org.rosuda.REngine.REXP;
import org.rosuda.REngine.REXPDouble;
import org.rosuda.REngine.REXPInteger;
import org.rosuda.REngine.REXPLogical;
import org.rosuda.REngine.REXPMismatchException;
import org.rosuda.REngine.REXPString;
import org.rosuda.REngine.REngine;
import org.rosuda.REngine.REngineException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.annotation.OutputPortFieldAnnotation;
import com.datatorrent.lib.script.ScriptOperator;

/**
 * This operator enables a user to execute a R script on tuples for Map<String, Object>. The script should be in the
 * form of a function. This function will then be called by the operator.
 *
 * The user should - 1. set the name of the script file (which contains the script in the form of a function) 2. set the
 * function name. 3. set the name of the return variable 4. Make sure that the script file is available in the
 * classpath. 5. set the type of arguments being passed. This will be done in a Map. 6. Send the data in the form of a
 * tuple consisting of a key:value pair where, "key" represents the name of the argument "value" represents the actual
 * value of the argument. A map of all the arguments is created and passed as input. The result will be returned on one
 * of the output ports depending on the type of the return value.
 *
 * <b> Sample Usage Code : </b> oper is an object of type RScript. Create it by passing - <name of the R script with
 * path from classpath>, <name of the function to be invoked>, <name of the return variable>);
 *
 * Map<String, RScript.REXP_TYPE> argTypeMap = new HashMap<String, RScript.REXP_TYPE>(); argTypeMap.put(<argument name>,
 * RScript.<argument type in the form of REXP_TYPE>); argTypeMap.put(<argument name>, RScript.<argument type in the form
 * of REXP_TYPE>); ... ...
 *
 * oper.setArgTypeMap(argTypeMap);
 *
 * HashMap map = new HashMap();
 *
 * map.put("<argument name>", <argument value>); map.put("<argument name>", <argument value>); ... ...
 *
 * Note that the number of arguments inserted into the map should be same in number and order as that mentioned in the
 * argument type map above it.
 *
 * Pass this 'map' to the operator now.
 *
 * Currently, support has been added for only int, real, string and boolean type of values and the corresponding arrays
 * to be passed and returned from the R scripts.
 *
 *
 *
 * */

public class RScript extends ScriptOperator
{

  @SuppressWarnings("unused")
  private static final long serialVersionUID = 201401161205L;

  public Map<String, REXP_TYPE> getArgTypeMap()
  {
    return argTypeMap;
  }

  public void setArgTypeMap(Map<String, REXP_TYPE> argTypeMap)
  {
    this.argTypeMap = argTypeMap;
  }

  public enum REXP_TYPE {
    REXP_INT(1), REXP_DOUBLE(2), REXP_STR(3), REXP_BOOL(6), REXP_ARRAY_INT(32), REXP_ARRAY_DOUBLE(33), REXP_ARRAY_STR(34), REXP_ARRAY_BOOL(36);

    private int value;

    REXP_TYPE(int value)
    {
      this.value = value;
    }
  }

  @NotNull
  private Map<String, REXP_TYPE> argTypeMap;

  // Name of the return variable
  private String returnVariable;

  // Function name given to the script inside the script file.
  private String functionName;

  protected String scriptFilePath;

  private transient REngine rengine;
  private static Logger log = LoggerFactory.getLogger(RScript.class);

  public RScript()
  {
  }

  public RScript(String rScriptFilePath, String rFunction, String returnVariable)
  {
    this.setScriptFilePath(rScriptFilePath);
    super.setScript(readFileAsString());
    this.setFunctionName(rFunction);
    this.setReturnVariable(returnVariable);
  }

  @Override
  public Map<String, Object> getBindings()
  {
    return null; // To change body of implemented methods use File | Settings | File Templates.
  }

  // Get the value of the name of the variable being returned
  public String getReturnVariable()
  {
    return returnVariable;
  }

  // Set the name for the return variable
  public void setReturnVariable(String returnVariable)
  {
    this.returnVariable = returnVariable;
  }

  // Get the value of the script file with path as specified.
  public String getScriptFilePath()
  {
    return scriptFilePath;
  }

  // Set the value of the script file which should be executed.
  public void setScriptFilePath(String scriptFilePath)
  {
    this.scriptFilePath = scriptFilePath;
  }

  public String getFunctionName()
  {
    return functionName;
  }

  public void setFunctionName(String functionName)
  {
    this.functionName = functionName;
  }

  // Output port on which an int type of value is returned.
  @OutputPortFieldAnnotation(name = "intOutput")
  public final transient DefaultOutputPort<Integer> intOutput = new DefaultOutputPort<Integer>();

  // Output port on which an double type of value is returned.
  @OutputPortFieldAnnotation(name = "doubleOutput")
  public final transient DefaultOutputPort<Double> doubleOutput = new DefaultOutputPort<Double>();

  // Output port on which an string type of value is returned.
  @OutputPortFieldAnnotation(name = "strOutput")
  public final transient DefaultOutputPort<String> strOutput = new DefaultOutputPort<String>();

  // Output port on which a boolean type of value is returned.
  @OutputPortFieldAnnotation(name = "boolOutput")
  public final transient DefaultOutputPort<Boolean> boolOutput = new DefaultOutputPort<Boolean>();

  // Output port on which an array of type int is returned.
  @OutputPortFieldAnnotation(name = "intArrayOutput")
  public final transient DefaultOutputPort<Integer[]> intArrayOutput = new DefaultOutputPort<Integer[]>();

  // Output port on which an array of type double is returned.
  @OutputPortFieldAnnotation(name = "doubleArrayOutput")
  public final transient DefaultOutputPort<Double[]> doubleArrayOutput = new DefaultOutputPort<Double[]>();

  // Output port on which an array of type str is returned.
  @OutputPortFieldAnnotation(name = "strArrayOutput")
  public final transient DefaultOutputPort<String[]> strArrayOutput = new DefaultOutputPort<String[]>();

  // Output port on which an array of type boolean is returned.
  @OutputPortFieldAnnotation(name = "boolArrayOutput")
  public final transient DefaultOutputPort<Boolean[]> boolArrayOutput = new DefaultOutputPort<Boolean[]>();

  /**
   * Process the tuples
   */
  @Override
  public void process(Map<String, Object> tuple)
  {
    processTuple(tuple);
  }

  /*
   * Initialize the R engine, set the name of the script file to be executed. If the script file to be executed on each
   * node is to be copied by this operator, do so.
   */
  @Override
  public void setup(Context.OperatorContext context)
  {
    super.setup(context);

    try {
      String[] args = { "--vanilla" };
      this.rengine = REngine.getLastEngine();
      if (this.rengine == null) {
        // new R-engine
        this.rengine = REngine.engineForClass("org.rosuda.REngine.JRI.JRIEngine", args, null, false);
        log.info("Creating new Rengine");
      } else {
        log.info("Got last Rengine");
      }
      REXP result = rengine.parseAndEval(super.script);
    } catch (REngineException e) {
      log.error("Exception: ", e);
      e.printStackTrace();
      throw new RuntimeErrorException(null, e.toString());
    } catch (REXPMismatchException e) {
      log.error("Exception: ", e);
      e.printStackTrace();
      throw new RuntimeErrorException(null, e.toString());
    } catch (Exception exc) {
      log.error("Exception: ", exc);
      throw new RuntimeErrorException(null, exc.toString());
    }
  }

  /*
   * Stop the R engine and delete the script file if it was copied by this operator during the initial setup.
   */
  @Override
  public void teardown()
  {

    if (rengine != null) {
      rengine.close();
    }
  }

  /*
   * This function reads the script file - the R script file here, which is to be executed and loads it into the memory.
   */
  private String readFileAsString()
  {
    StringBuffer fileData = new StringBuffer(1000);

    try {

      BufferedReader reader;
      reader = new BufferedReader(new InputStreamReader(this.getClass().getClassLoader().getResourceAsStream(this.scriptFilePath)));

      char[] buf = new char[1024];
      int numRead = 0;

      while ((numRead = reader.read(buf)) != -1) {
        String readData = String.valueOf(buf, 0, numRead);
        fileData.append(readData);
      }

      reader.close();
    } catch (IOException ex) {
      log.error(String.format("\nError reading the R script"));
      ex.printStackTrace();
    }
    return fileData.toString();

  }

  /**
   * Execute R code with variable value map. Here,the RScript will be called for each of the tuples.The data will be
   * emitted on an outputport depending on its type. It is assumed that the downstream operator knows the type of data
   * being emitted by this operator and will be receiving input tuples from the right output port of this operator.
   */

  public void processTuple(Map<String, Object> tuple)
  {

    try {
      for (Map.Entry<String, Object> entry : tuple.entrySet()) {
        String key = entry.getKey();
        Object value = entry.getValue();

        switch (argTypeMap.get(key)) {
        case REXP_INT:
          int[] iArr = new int[1];
          iArr[0] = (Integer) value;
          rengine.assign(key, new REXPInteger(iArr));
          break;
        case REXP_DOUBLE:
          double[] dArr = new double[1];
          dArr[0] = (Double) value;
          rengine.assign(key, new REXPDouble(dArr));
          break;
        case REXP_STR:
          String[] sArr = new String[1];
          sArr[0] = (String) value;
          rengine.assign(key, new REXPString(sArr));
          break;
        case REXP_BOOL:
          Boolean[] bArr = new Boolean[1];
          bArr[0] = (Boolean) value;
          rengine.assign(key, new REXPLogical(bArr[0]));
          break;
        case REXP_ARRAY_INT:
          rengine.assign(key, new REXPInteger((int[]) value));
          break;
        case REXP_ARRAY_DOUBLE:
          rengine.assign(key, new REXPDouble((double[]) value));
          break;
        case REXP_ARRAY_STR:
          rengine.assign(key, new REXPString((String[]) value));
          break;
        case REXP_ARRAY_BOOL:
          rengine.assign(key, new REXPLogical((boolean[]) value));
          break;
        default:
          throw new IllegalArgumentException("Unsupported data type ... ");
        }
      }

      REXP result = rengine.parseAndEval(getReturnVariable() + "<-" + getFunctionName() + "()");
      REXP retVal = rengine.parseAndEval(getReturnVariable());
      // Clear R workspace, except functions.
      rengine.parseAndEval("rm(list = setdiff(ls(), lsf.str()))");

      // Get the returned value and emit it on the appropriate output port depending
      // on its datatype.
      int len = 0;

      if (retVal.isInteger()) {
        len = retVal.length();

        if (len > 1) {
          Integer[] iAList = new Integer[len];
          for (int i = 0; i < len; i++) {
            iAList[i] = (retVal.asIntegers()[i]);
          }
          intArrayOutput.emit(iAList);
        } else {
          intOutput.emit(retVal.asInteger());
        }
      } else if (retVal.isNumeric()) {

        len = retVal.length();

        if (len > 1) {
          Double[] dAList = new Double[len];
          for (int i = 0; i < len; i++) {
            dAList[i] = (retVal.asDoubles()[i]);
          }
          doubleArrayOutput.emit(dAList);
        } else {
          doubleOutput.emit(retVal.asDouble());
        }
      } else if (retVal.isString()) {
        len = retVal.length();

        if (len > 1) {
          strArrayOutput.emit(retVal.asStrings());

        } else {
          strOutput.emit(retVal.asString());
        }
      } else if (retVal.isLogical()) {
        len = retVal.length();
        boolean[] bData = new boolean[len];

        if (len > 1) {
          Boolean[] bAList = new Boolean[len];
          for (int i = 0; i < len; i++) {
            bAList[i] = ((REXPLogical) retVal).isTRUE()[i];
          }

          boolArrayOutput.emit(bAList);
        } else {
          bData = (((REXPLogical) retVal).isTRUE());
          boolOutput.emit(bData[0]);
        }

      } else {
        throw new IllegalArgumentException("Unsupported data type returned ... ");
      }
    } catch (Exception exc) {
      log.error("Rexc: ", exc);
      exc.printStackTrace();
    }
  }

}
