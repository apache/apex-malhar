package org.apache.apex.malhar.lib.fs;

import org.apache.apex.api.ControlAwareDefaultInputPort;
import org.apache.apex.api.UserDefinedControlTuple;
import org.apache.apex.malhar.lib.window.Tuple;
import org.apache.apex.malhar.lib.window.windowable.FileWatermark;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.common.util.BaseOperator;
import com.datatorrent.lib.util.KeyValPair;

public class StringToKeyValPair extends BaseOperator
{
  long timestamp;

  public final transient ControlAwareDefaultInputPort<String> input = new ControlAwareDefaultInputPort<String>()
  {
    @Override
    public boolean processControl(UserDefinedControlTuple tuple)
    {
      if (tuple instanceof FileWatermark.BeginFileWatermark) {
        timestamp = ((FileWatermark.BeginFileWatermark)tuple).getTimestamp();
      } else if (tuple instanceof FileWatermark.EndFileWatermark) {
        timestamp = ((FileWatermark.EndFileWatermark)tuple).getTimestamp();
      }
      return false;
    }

    @Override
    public void process(String tuple)
    {
      output.emit(new Tuple.TimestampedTuple<KeyValPair<String, Long>>(timestamp, new KeyValPair<String, Long>(tuple, 1L)));
    }
  };

  public final transient DefaultOutputPort<Tuple<KeyValPair<String, Long>>> output = new DefaultOutputPort<>();
}
