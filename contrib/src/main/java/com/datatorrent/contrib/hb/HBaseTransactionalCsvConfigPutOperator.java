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
package com.datatorrent.contrib.hb;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.supercsv.io.CsvListReader;
import org.supercsv.io.ICsvListReader;
import org.supercsv.prefs.CsvPreference;

import com.datatorrent.api.Context.OperatorContext;
import com.datatorrent.common.util.DTThrowable;

/**
 * accepts a string of key value pairs containing the data to be inserted.These
 * are mapped to corresponding rows,columnfamilies and columns using a property
 * file and inserted into hbase Eg: input string will be of the form
 * name=xyz,st=patrickhenry,ct=fremont,sa=california the properties will contain
 * properties of form
 * name=row,sa=address.street,ct=address.city,sa=address.state with the above
 * the mapping a row xyz is created. the value patrickhenry is inserted into
 * columnfamily address and column street of row xyz. other values are inserted
 * similarly.
 */
@SuppressWarnings("serial")
public class HBaseTransactionalCsvConfigPutOperator extends
		AbstractHBaseTransactionalPutOutputOperator<String> {
	private static final transient Logger logger = LoggerFactory
			.getLogger(HBaseTransactionalCsvConfigPutOperator.class);
	private transient Properties prop;
	private String propFilepath;

	public String getPropFilepath() {
		return propFilepath;
	}

	public void setPropFilepath(String propFilepath) {
		this.propFilepath = propFilepath;
	}

	@Override
	public Put operationPut(String t) {
		return parseLine(t);

	}

	public Put parseLine(String s) {
		Put put = null;
		ICsvListReader listReader = null;
		ArrayList<String> csvList = new ArrayList<String>();
		Map<String, String> linemap = new HashMap<String, String>();

		try {
			listReader = new CsvListReader(new StringReader(s),
					CsvPreference.STANDARD_PREFERENCE);

			csvList = (ArrayList<String>) listReader.read();
		} catch (IOException e) {
			logger.error("Cannot read the property string" + e.getMessage());
			DTThrowable.rethrow(e);
		} finally {
			try {
				listReader.close();
			} catch (IOException e) {
				logger.error("Error closing Csv reader" + e.getMessage());
				DTThrowable.rethrow(e);
			}
		}
		for (String pair : csvList) {
			linemap.put(pair.substring(0, pair.indexOf('=')),
					pair.substring(pair.indexOf('=') + 1));
		}
		for (Entry<String, String> pair : linemap.entrySet()) {

			if (prop.getProperty(pair.getKey()).equals("row")) {
				put = new Put(pair.getValue().getBytes());
				linemap.remove(pair.getKey());
				break;
			}
		}
		for (Entry<String, String> pair : linemap.entrySet()) {
			String propVal = prop.getProperty(pair.getKey());
			put.add(propVal.substring(0, propVal.indexOf('.')).getBytes(),
					propVal.substring(propVal.indexOf('.') + 1).getBytes(),
					pair.getValue().getBytes());
		}
		return put;
	}

	public void loadPropertyFile() {
		prop = new Properties();
		FSDataInputStream input = null;
		FileSystem fs = null;
		try {
			Configuration conf = new Configuration();
			fs = FileSystem.get(conf);

			Path path = new Path(propFilepath);
			if (!fs.exists(path)) {
				logger.error("property file does not exist");
				throw new RuntimeException("Property file does not exist");
			}
			input = fs.open(path);
			prop.load(input);
		} catch (IOException e) {
			logger.error("Error loading property file" + e.getMessage());
			DTThrowable.rethrow(e);
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.error("Error closing FSDataInputStream"
							+ e.getMessage());
					DTThrowable.rethrow(e);
				}
			}
		}
	}

	@Override
	public void setup(OperatorContext context) {
		super.setup(context);
		loadPropertyFile();
	}

}
