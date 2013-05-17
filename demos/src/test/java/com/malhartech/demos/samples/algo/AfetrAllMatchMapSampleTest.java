package com.malhartech.demos.samples.algo;

import com.malhartech.demos.samples.math.AverageKeyValMapSample;
import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


public class AfetrAllMatchMapSampleTest {
	
	  @Test
	  public void testSomeMethod() throws Exception
	  {
		  AverageKeyValMapSample topology = new AverageKeyValMapSample();
		  final StramLocalCluster lc = new StramLocalCluster(topology.getApplication(new Configuration(false)));

		  new Thread("LocalClusterController")
		  {
			  @Override
			  public void run()
			  {
				  try {
					  Thread.sleep(120000);
				  }	catch (InterruptedException ex) {
				  }
				  lc.shutdown();
			  }

	    }.start();
	    lc.run();
	  }
}