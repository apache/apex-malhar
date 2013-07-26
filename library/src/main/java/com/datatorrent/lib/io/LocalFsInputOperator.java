/*
 * Copyright (c) 2013 Malhar Inc. ALL Rights Reserved.
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
package com.datatorrent.lib.io;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;

import com.datatorrent.api.DefaultOutputPort;
import com.datatorrent.api.Context.OperatorContext;

/**
 * Operator to read input tuples from file from local file system.
 *
 * @since 0.3.2
 */
public class LocalFsInputOperator extends AbstractLocalFSInputOperator
{
	public final transient DefaultOutputPort<String> outport = new DefaultOutputPort<String>();
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

	@Override
	public void emitTuples()
	{
		emitTuples(null);
	}
}
