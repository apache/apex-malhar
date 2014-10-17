package com.datatorrent.demos.oldfaithful;

import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;

import com.datatorrent.api.DAG;
import com.datatorrent.api.StreamingApplication;
import com.datatorrent.api.annotation.ApplicationAnnotation;
import com.datatorrent.lib.io.ConsoleOutputOperator;

//The application attempts to simulate 'Old Faithful Geyser" eruption.
//This application accepts readings for the waiting time and the subsequent eruption duration
//of the 'Old Faithful' and based on this data, tries to predict the eruption duration of the next
//eruption given the elapsed time since the last eruption.
//The training data is generated for an application window and consists of multiple
//waiting times and eruption duration values.
//For every application window, it generates only one 'elapsed time' input for which the
//prediction would be made.
// Model in R is in file ruptionModel.R located at 
// demos/r/src/main/resources/com/datatorrent/demos/oldfaithful/ directory

@ApplicationAnnotation(name = "OldFaithfulApplication")
public class OldFaithfulApplication implements StreamingApplication
{
  private final DAG.Locality locality = null;

  /**
   * Create the DAG
   */
  @Override
  public void populateDAG(DAG dag, Configuration conf)
  {

    dag.setAttribute(DAG.APPLICATION_NAME, "OldFaithfulApplication");
    InputGenerator randomInputGenerator = dag.addOperator("rand", new InputGenerator());
    FaithfulRScript rScriptOp = dag.addOperator("rScriptOp", new FaithfulRScript("com/datatorrent/demos/oldfaithful/eruptionModel.R", "eruptionModel", "retVal"));
    ConsoleOutputOperator consoles = dag.addOperator("consoles", new ConsoleOutputOperator());

    Map<String, FaithfulRScript.REXP_TYPE> argTypeMap = new HashMap<String, FaithfulRScript.REXP_TYPE>();

    argTypeMap.put("ELAPSEDTIME", FaithfulRScript.REXP_TYPE.REXP_INT);
    argTypeMap.put("ERUPTIONS", FaithfulRScript.REXP_TYPE.REXP_ARRAY_DOUBLE);
    argTypeMap.put("WAITING", FaithfulRScript.REXP_TYPE.REXP_ARRAY_INT);

    rScriptOp.setArgTypeMap(argTypeMap);

    dag.addStream("ingen_faithfulRscript", randomInputGenerator.outputPort, rScriptOp.faithfulInput).setLocality(locality);
    dag.addStream("ingen_faithfulRscript_eT", randomInputGenerator.elapsedTime, rScriptOp.inputElapsedTime).setLocality(locality);
    dag.addStream("faithfulRscript_console_s", rScriptOp.strOutput, consoles.input).setLocality(locality);

  }
}
