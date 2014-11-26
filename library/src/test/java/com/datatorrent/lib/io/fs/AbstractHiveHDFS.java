package com.datatorrent.lib.io.fs;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.api.DAG;
import com.datatorrent.api.Operator.CheckpointListener;
import java.util.HashMap;

public abstract class AbstractHiveHDFS<T> implements CheckpointListener
{
  protected HashMap<String, Long> filenames;
  private transient String appId;
  private transient int operatorId;
  public HDFSRollingOutputOperator hdfsOp;
  public String filepath;
  protected String tablename;
  private long checkpointedWindowId = -1;
  private long committedWindowId = -1;

  public String getFilepath()
  {
    return filepath;
  }

  public void setFilepath(String filepath)
  {
    this.filepath = filepath;
  }

  public AbstractHiveHDFS()
  {
    hdfsOp = new HDFSRollingOutputOperator();
    filenames = new HashMap<String, Long>();
    hdfsOp.hive = (HiveInsertOperator)this;
  }

  public void processTuple(T tuple)
  {
    hdfsOp.input.process(tuple);
  }

  public void setup(OperatorContext context)
  {
    appId = context.getValue(DAG.APPLICATION_ID);
    operatorId = context.getId();
    hdfsOp.setFilePath(filepath + "/" + appId + "/" + operatorId);
    hdfsOp.setup(context);
  }

  public void teardown()
  {
    hdfsOp.teardown();
  }

  public void beginWindow(long windowId)
  {
    hdfsOp.beginWindow(windowId);
  }

  public void endWindow()
  {
    hdfsOp.endWindow();
  }

  public String getHiveTuple(T tuple)
  {
    return tuple.toString() + "\n";
  }

  public String getTablename()
  {
    return tablename;
  }

  public void setTablename(String tablename)
  {
    this.tablename = tablename;
  }

  @Override
  public void checkpointed(long windowId)
  {
    checkpointedWindowId = windowId;
  }

  @Override
  public void committed(long windowId)
  {
    committedWindowId = windowId;
  }

}
