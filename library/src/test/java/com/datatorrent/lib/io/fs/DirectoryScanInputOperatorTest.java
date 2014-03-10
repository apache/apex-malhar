/*
 * Copyright (c) 2013 DataTorrent, Inc. ALL Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.datatorrent.lib.io.fs;



import java.io.File;
import java.io.IOException;
import com.datatorrent.lib.testbench.CollectorTestSink;
import junit.framework.Assert;
import org.apache.commons.io.FileUtils;
import org.junit.Test;

/**
 * Units tests for DirectorySanInputOperator
 */
public class DirectoryScanInputOperatorTest
{
	// Sample text file path.
	protected String dirName = "../library/src/test/resources/directoryScanTestDirectory";


	@SuppressWarnings({ "rawtypes", "unchecked" })
  @Test
	public void testDirectoryScan() throws InterruptedException, IOException
	{
	  
	  //Create the directory and the files used in the test case
	  File baseDir = new File(dirName);

	  // if the directory does not exist, create it
	  if (!baseDir.exists()) {
	    System.out.println("creating directory: " + dirName);
	    boolean result = baseDir.mkdir();  

	     if(result) {    
	       System.out.println("base directory created");  
	       
	       //create sub-directory
	       
	       File subDir = new File(dirName + "/subDir");

	       // if the directory does not exist, create it
	       if (!subDir.exists()) {
	         System.out.println("creating subdirectory: " + dirName + "/subDir");
	         boolean subDirResult = subDir.mkdir();  

	          if(subDirResult) {    
	            System.out.println("sub directory created"); 
	          }
	       }	       
	     }
	     else
	     {
	       System.out.println("Failed to create base directory");
	       return;	       
	     }
	  }
	  
	  //Create the files inside base dir	  
	  int numOfFilesInBaseDir = 4;
	  for(int num = 0; num < numOfFilesInBaseDir; num++)
	  {
	    String fileName = dirName+"/testFile"+ String.valueOf(num+1) +  ".txt";
	    File file = new File(fileName);
	 	  
      if (file.createNewFile()){
        System.out.println(fileName + " is created!");
        FileUtils.writeStringToFile(file, fileName);
        
      }else{
        System.out.println(fileName + " already exists.");
      }
	  }
	  
    //Create the files inside base subDir  
	  int numOfFilesInSubDir = 6;
    for(int num = 0; num < numOfFilesInSubDir; num++)
    {
      String fileName = dirName+ "/subDir/testFile"+ String.valueOf(num + numOfFilesInBaseDir +1) +  ".txt";
      File file = new File(fileName);
      
      if (file.createNewFile()){
        System.out.println(fileName + " is created!");
        FileUtils.writeStringToFile(file, fileName);
      }else{
        System.out.println(fileName + " already exists.");
      }
    }
    
    
	  
		DirectoryScanInputOperator oper = new DirectoryScanInputOperator();
		oper.setDirectoryPath(dirName);
		oper.setScanIntervalInMilliSeconds(1000);
		oper.activate(null);

		CollectorTestSink sink = new CollectorTestSink();
		oper.outport.setSink(sink);
		
		//Test if the existing files in the directory are emitted.
		Thread.sleep(1000);
		oper.emitTuples();
    
    Assert.assertTrue("tuple emmitted: " + sink.collectedTuples.size(), sink.collectedTuples.size() > 0);
		Assert.assertEquals(sink.collectedTuples.size(), 10);
		
		
		//Test if the new file added is detected
		sink.collectedTuples.clear();
		
		File oldFile = new File("../library/src/test/resources/directoryScanTestDirectory/testFile1.txt");
		File newFile = new File("../library/src/test/resources/directoryScanTestDirectory/newFile.txt");
		FileUtils.copyFile(oldFile, newFile);

		int timeoutMillis = 2000;
    while (sink.collectedTuples.isEmpty() && timeoutMillis > 0) {
      oper.emitTuples();
      timeoutMillis -= 20;
      Thread.sleep(20);
    }
    		
    Assert.assertTrue("tuple emmitted: " + sink.collectedTuples.size(), sink.collectedTuples.size() > 0);
    Assert.assertEquals(sink.collectedTuples.size(), 1);
    
    //clean up the directory to initial state
    newFile.delete();    
		
    oper.deactivate();
		oper.teardown();
		
		//clean up the directory used for the test		
		FileUtils.deleteDirectory(baseDir);
	}

}
