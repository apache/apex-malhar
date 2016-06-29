package org.apache.apex.malhar.stream.sample;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import com.google.common.base.Throwables;

import com.datatorrent.api.Context;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.InputOperator;
import com.datatorrent.common.util.BaseOperator;

public class WCInput extends BaseOperator implements InputOperator
{
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();

  private transient BufferedReader reader;

  @Override
  public void setup(Context.OperatorContext context)
  {
    initReader();
  }

  private void initReader()
  {
    try {
      Path myPath = new Path("/user/siyuan/wc/wordcount");
      FileSystem fs = FileSystem.get(new Configuration());
      reader = new BufferedReader(new InputStreamReader(fs.open(myPath)));
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  @Override
  public void teardown()
  {
    IOUtils.closeQuietly(reader);
  }

  @Override
  public void emitTuples()
  {
    try {
      String line = reader.readLine();
      if (line == null) {
        reader.close();
        initReader();
      } else {
        // simulate late data
        //long timestamp = System.currentTimeMillis() - (long)(Math.random() * 30000);

        this.output.emit(line);
      }
    } catch (IOException ex) {
      throw new RuntimeException(ex);
    }
  }

  @Override
  public void endWindow()
  {
    //this.controlOutput.emit(new WatermarkImpl(System.currentTimeMillis() - 15000));
  }
}