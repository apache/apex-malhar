package com.malhartech.lib.io;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import com.malhartech.api.ApplicationFactory;
import com.malhartech.api.DAG;
import com.malhartech.api.DefaultOutputPort;
import com.malhartech.api.Context.OperatorContext;
import com.malhartech.lib.io.*;
import com.malhartech.lib.math.Average;
import com.malhartech.lib.testbench.RandomEventGenerator;
import org.apache.hadoop.conf.Configuration;

/**  
 * <b> Usage Operator : </b> com.malhartech.lib.testbench.RandomEventGenerator <br>
 * This sample usage for predefined operator <b>RandomEventGenerator</b>. <br>
 * Random generator output is printed to output console(can be any downstream operator).
 * 
 * @author Dinesh Prasad(dinesh@malhar-inc.com)
 */
public class LocalFsInputOperator extends AbstractLocalFSInputOperator
{
	public final transient DefaultOutputPort<String> outport = new DefaultOutputPort<String>(this);
	private DataInputStream in;
	private BufferedReader br;
	private int sleepInterval = 0;
	
	@Override
	public void setup(OperatorContext context)
	{
		input = openFile(getFilePath());
		in = new DataInputStream(input);
		br = new BufferedReader(new InputStreamReader(in)); 
	}
	
	@Override
	public void teardown()
	{
		try {
			input.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	@Override
	public void emitTuples(FileInputStream stream) {
		try{    
			String strLine = br.readLine();  
			if(strLine != null) outport.emit(strLine);
		}catch (Exception e){
			e.printStackTrace();
		}
		if (sleepInterval > 0) {
			try {
				Thread.sleep(sleepInterval);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}
	}

	public int getSleepInterval() {
		return sleepInterval;
	}

	public void setSleepInterval(int sleepInterval) {
		this.sleepInterval = sleepInterval;
	}
}
