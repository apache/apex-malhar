package com.malhartech.demos.samples.io;

import com.malhartech.demos.samples.io.LocalFsInputSample;
import com.malhartech.stram.StramLocalCluster;
import org.apache.hadoop.conf.Configuration;
import org.junit.Test;


public class LocalFSInputSampleTest {
	
	  @Test
	  public void testSomeMethod() throws Exception
	  {
		  LocalFsInputSample topology = new LocalFsInputSample();
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