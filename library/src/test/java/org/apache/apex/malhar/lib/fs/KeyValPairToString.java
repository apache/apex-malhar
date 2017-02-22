package org.apache.apex.malhar.lib.fs;

import org.apache.apex.malhar.lib.window.Tuple;

import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

public class KeyValPairToString extends BaseOperator
{
  public final transient DefaultInputPort<Tuple<KeyValPair<String,Long>>> input =
      new DefaultInputPort<Tuple<KeyValPair<String, Long>>>()
  {
    @Override
    public void process(Tuple<KeyValPair<String, Long>> tuple)
    {
      StringBuffer buffer = new StringBuffer();
      KeyValPair t = tuple.getValue();
      output.emit(buffer.append(t.getKey()).append(" -> ").append(t.getValue()).append("\n").toString());
    }
  };
  public final transient DefaultOutputPort<String> output = new DefaultOutputPort<>();
}
