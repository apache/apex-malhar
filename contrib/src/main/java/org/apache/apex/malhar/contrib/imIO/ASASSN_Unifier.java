package org.apache.apex.malhar.contrib.imIO;
/*
 * imIO5.1
 * Created by Aditya Gholba on 25/4/17.
 */
import java.util.ArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import com.datatorrent.api.DefaultInputPort;
import com.datatorrent.api.DefaultOutputPort;


public class ASASSN_Unifier extends ToolKit
{
  private static final Logger LOG = LoggerFactory.getLogger(ASASSN_Unifier.class);
  public final transient DefaultOutputPort<Data> outputOther = new DefaultOutputPort<>();
  transient ArrayList<String> imageList = new ArrayList<>();
  transient ArrayList<Data2> data2ArrayList = new ArrayList<>();
  public final transient DefaultInputPort<Data2> input = new DefaultInputPort<Data2>()
  {

    @Override
    public void process(Data2 tuple)
    {
      processTuple(tuple);
    }
  };
  public final transient DefaultInputPort<Data2> input1 = new DefaultInputPort<Data2>()
  {

    @Override
    public void process(Data2 tuple)
    {
      processTuple(tuple);
    }
  };
  public final transient DefaultInputPort<Data2> input2 = new DefaultInputPort<Data2>()
  {

    @Override
    public void process(Data2 tuple)
    {
      processTuple(tuple);
    }
  };

  void processTuple(Data2 data)
  {
    synchronized (this.data2ArrayList) {
      synchronized (this.imageList) {
        if (imageList.contains(data.fileName)) {
          for (Data2 partData : data2ArrayList) {
            if (partData.fileName.equalsIgnoreCase(data.fileName)) {
              partData.matches = partData.matches + data.matches;
              partData.parts++;
            }
            LOG.info("Exists in list" + partData.fileName + " part " + partData.parts + " matches " + partData.matches);
          }
        } else {
          imageList.add(data.fileName);
          data2ArrayList.add(data);
          LOG.info("Added to list" + data.fileName + " part " + data.parts + " matches " + data.matches);
        }
      }
    }
    ArrayList<Data2> removeList = new ArrayList<>();
    synchronized (this.data2ArrayList) {
      synchronized (this.imageList) {

        for (Data2 partData : data2ArrayList) {
          if (partData.parts == 2 && partData.sent == 0) {
            Data data2 = new Data();
            data2.bytesImage = partData.bytesImage;
            data2.fileName = partData.fileName;
            removeList.add(partData);
            imageList.remove(partData.fileName);
            partData.sent++;
            if (partData.matches >= (partData.dense * 0.1) && partData.matches >= 10) {
              // data2ArrayList.remove(partData);
              LOG.info("Matches C " + partData.fileName + " matches " + partData.matches + " dense " + partData.dense +
                  " parts " + partData.parts);
              output.emit(data2);
            } else {
              //data2ArrayList.remove(partData);
              LOG.info("Matches O " + partData.fileName + " matches " + partData.matches + " dense " + partData.dense +
                  " parts " + partData.parts);
              outputOther.emit(data2);
            }
          }
        }
      }
    }
    for (Data2 remove : removeList) {
      data2ArrayList.remove(remove);
    }

  }
}
