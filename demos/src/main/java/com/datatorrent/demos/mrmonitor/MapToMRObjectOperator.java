package com.datatorrent.demos.mrmonitor;

import java.util.HashMap;
import java.util.Map;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Operator;

public class MapToMRObjectOperator implements Operator
{

  public final transient DefaultInputPort<Map<String, String>> input = new DefaultInputPort<Map<String, String>>() {
    @Override
    public void process(Map<String, String> tuple)
    {
      MRStatusObject mrStatusObj = new MRStatusObject();

      for (Map.Entry<String, String> e : tuple.entrySet()) {
        if (e.getKey().equals(Constants.QUERY_KEY_COMMAND)) {
          mrStatusObj.setCommand(e.getValue());
        } else if (e.getKey().equals(Constants.QUERY_API_VERSION)) {
          mrStatusObj.setApiVersion(e.getValue());
        } else if (e.getKey().equals(Constants.QUERY_APP_ID)) {
          mrStatusObj.setAppId(e.getValue());
        } else if (e.getKey().equals(Constants.QUERY_HADOOP_VERSION)) {
          mrStatusObj.setHadoopVersion(Integer.parseInt(e.getValue()));
        } else if (e.getKey().equals(Constants.QUERY_HOST_NAME)) {
          mrStatusObj.setUri(e.getValue());
        } else if (e.getKey().equals(Constants.QUERY_HS_PORT)) {
          mrStatusObj.setHistoryServerPort(Integer.parseInt(e.getValue()));
        } else if (e.getKey().equals(Constants.QUERY_JOB_ID)) {
          mrStatusObj.setJobId(e.getValue());
        } else if (e.getKey().equals(Constants.QUERY_RM_PORT)) {
          mrStatusObj.setRmPort(Integer.parseInt(e.getValue()));
        }
      }
      output.emit(mrStatusObj);

    }
  };

  public final transient DefaultOutputPort<MRStatusObject> output = new DefaultOutputPort<MRStatusObject>();

  @Override
  public void setup(OperatorContext context)
  {
  }

  @Override
  public void teardown()
  {
  }

  @Override
  public void beginWindow(long windowId)
  {
  }

  @Override
  public void endWindow()
  {
  }

}
