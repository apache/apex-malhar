package org.apache.apex.malhar;

/**
 * Created by vikram on 26/6/17.
 */
public class PythonConstants
{

  public enum OpType
  {
    MAP("MAP"),
    FLAT_MAP("FLAT_MAP"),
    FILTER("FILTER"),
    REDUCE("REDUCE"),
    REDUCE_BY_KEY("REDUCE_BY_KEY");

    private String operationName = null;

    OpType(String name)
    {
      this.operationName = name;
    }

    public String getType()
    {
      return operationName;
    }

  }
  public static String PY4J_SRC_ZIP_FILE_NAME = "py4j-0.10.4-src.zip";
  public static String PYTHON_WORKER_FILE_NAME = "worker.py";
  public static String PYTHON_APEX_ZIP_NAME = "pyapex-0.0.4-src.zip";
}
