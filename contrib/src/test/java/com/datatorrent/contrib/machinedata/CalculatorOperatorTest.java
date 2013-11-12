package com.datatorrent.contrib.machinedata;

import com.datatorrent.contrib.machinedata.data.MachineInfo;
import com.datatorrent.contrib.machinedata.data.MachineKey;
import com.datatorrent.contrib.machinedata.data.ResourceType;
import com.datatorrent.contrib.machinedata.operator.CalculatorOperator;
import com.datatorrent.lib.testbench.CollectorTestSink;
import com.datatorrent.lib.util.KeyValPair;
import com.datatorrent.lib.util.TimeBucketKey;
import com.google.common.collect.ImmutableList;

import junit.framework.Assert;

import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;

/**
 *
 * @since 0.3.5
 */
public class CalculatorOperatorTest {
  private static DateFormat minuteDateFormat = new SimpleDateFormat("HHmm");
    private static Logger LOG = LoggerFactory.getLogger(CalculatorOperatorTest.class);
    /**
     * Test node logic emits correct results
     */
    @Test
    public void testNodeProcessing() throws Exception {
        CalculatorOperator calculatorOperator= new CalculatorOperator() ;
        calculatorOperator.setup(null);

        calculatorOperator.setComputePercentile(true);
        calculatorOperator.setComputeMax(true);
        calculatorOperator.setComputeSD(true);

        testPercentile(calculatorOperator);
    }

    public void testPercentile(CalculatorOperator oper){

        CollectorTestSink sortSink = new CollectorTestSink();
        oper.percentileOutputPort.setSink(sortSink);
        oper.setKthPercentile(50);
        Calendar calendar = Calendar.getInstance(); 
        Date date = calendar.getTime();
        String timeKey = minuteDateFormat.format(date);
        String day = calendar.get(Calendar.DAY_OF_MONTH)+"";

        Integer vs= new Integer(1);
        MachineKey mk= new MachineKey(timeKey,day, vs, vs,vs,vs, vs,vs,vs) ;


        oper.beginWindow(0);

        MachineInfo info = new MachineInfo(mk, 1, 1, 1);
        oper.dataPort.process(info);

        info.setCpu(2);
        oper.dataPort.process(info);

        info.setCpu(3);
        oper.dataPort.process(info);

        oper.endWindow();

        Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
        for (Object o: sortSink.collectedTuples) {
            LOG.debug(o.toString());
            KeyValPair<TimeBucketKey, Map<ResourceType, Double>> keyValPair = (KeyValPair<TimeBucketKey, Map<ResourceType, Double>>) o;
            Assert.assertEquals("emitted value for 'cpu' was ", 2.0, keyValPair.getValue().get(ResourceType.CPU));
            Assert.assertEquals("emitted value for 'hdd' was ", 1.0, keyValPair.getValue().get(ResourceType.HDD));
            Assert.assertEquals("emitted value for 'ram' was ", 1.0, keyValPair.getValue().get(ResourceType.RAM));

        }
        LOG.debug("Done percentile testing\n");

    }

    public void testStandarDeviation(CalculatorOperator oper){
        CollectorTestSink sortSink = new CollectorTestSink();
        oper.sdOutputPort.setSink(sortSink);
        Calendar calendar = Calendar.getInstance();      
        Date date = calendar.getTime();
        String timeKey = minuteDateFormat.format(date);
        String day = calendar.get(Calendar.DAY_OF_MONTH)+"";

        Integer vs= new Integer(1);
        MachineKey mk= new MachineKey(timeKey,day, vs, vs,vs,vs, vs,vs,vs) ;


        oper.beginWindow(0);

        MachineInfo info = new MachineInfo(mk, 1, 1, 1);
        oper.dataPort.process(info);

        info.setCpu(2);
        oper.dataPort.process(info);

        info.setCpu(3);
        oper.dataPort.process(info);

        oper.endWindow();

        Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
        for (Object o: sortSink.collectedTuples) {
            LOG.debug(o.toString());
            KeyValPair<TimeBucketKey, Map<ResourceType, Double>> keyValPair = (KeyValPair<TimeBucketKey, Map<ResourceType, Double>>) o;
            Assert.assertEquals("emitted value for 'cpu' was ", getSD(ImmutableList.of(1, 2, 3)), keyValPair.getValue().get(ResourceType.CPU));
            Assert.assertEquals("emitted value for 'hdd' was ", getSD(ImmutableList.of(1,1,1)), keyValPair.getValue().get(ResourceType.HDD));
            Assert.assertEquals("emitted value for 'ram' was ", getSD(ImmutableList.of(1,1,1)), keyValPair.getValue().get(ResourceType.RAM));

        }
        LOG.debug("Done sd testing\n");

    }

    private final double getSD(List<Integer> input){
        int sum=0;
        for(int i: input) sum+=i;
        double avg= sum/(input.size()*1.0);
        double sd=0;
        for(Integer point: input){
            sd+= Math.pow(point-avg,2);
        }
        return Math.sqrt(sd);
    }

    public void testMax(CalculatorOperator oper){
        CollectorTestSink sortSink = new CollectorTestSink();
        oper.maxOutputPort.setSink(sortSink);
        Calendar calendar = Calendar.getInstance();      
        Date date = calendar.getTime();
        String timeKey = minuteDateFormat.format(date);
        String day = calendar.get(Calendar.DAY_OF_MONTH)+"";

        Integer vs= new Integer(1);
        MachineKey mk= new MachineKey(timeKey,day, vs, vs,vs,vs, vs,vs,vs) ;


        oper.beginWindow(0);

        MachineInfo info = new MachineInfo(mk, 1, 1, 1);
        oper.dataPort.process(info);

        info.setCpu(2);
        oper.dataPort.process(info);

        info.setCpu(3);
        oper.dataPort.process(info);

        oper.endWindow();

        Assert.assertEquals("number emitted tuples", 1, sortSink.collectedTuples.size());
        for (Object o: sortSink.collectedTuples) {
            LOG.debug(o.toString());
            KeyValPair<TimeBucketKey, Map<ResourceType, Double>> keyValPair = (KeyValPair<TimeBucketKey, Map<ResourceType, Double>>) o;
            Assert.assertEquals("emitted value for 'cpu' was ", 3, keyValPair.getValue().get(ResourceType.CPU));
            Assert.assertEquals("emitted value for 'hdd' was ", 1, keyValPair.getValue().get(ResourceType.HDD));
            Assert.assertEquals("emitted value for 'ram' was ", 1, keyValPair.getValue().get(ResourceType.RAM));

        }
        LOG.debug("Done max testing\n");

    }
}
