package com.malhartech.demos.samples.io;

import com.malhartech.demos.samples.io.HttpInputOperatorSample;
import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


public class HttpInputOperatorSampleTest {
	
	  @Test
	  public void testSomeMethod() throws Exception
	  {
		  HttpInputOperatorSample topology = new HttpInputOperatorSample();
	    final StramLocalCluster lc = new StramLocalCluster(topology.getApplication(new Configuration(false)));

	    new Thread("LocalClusterController")
	    {
	      @Override
	      public void run()
	      {
	        try {
	          Thread.sleep(120000);
	        }
	        catch (InterruptedException ex) {
	        }

	        lc.shutdown();
	      }

	    }.start();

	    lc.run();
	  }
}