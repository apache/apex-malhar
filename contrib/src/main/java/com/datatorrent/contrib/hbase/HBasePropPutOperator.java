package com.datatorrent.contrib.hbase;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map.Entry;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.client.Put;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.Context.OperatorContext;

/**
 * accepts a string of key value pairs containing the data to be inserted.These are mapped to corresponding rows,columnfamilies and 
 * columns using a property file and inserted into hbase
 * Eg:
 * input string will be of the form name=xyz,st=patrickhenry,ct=fremont,sa=california
 * the properties will contain properties of form name=row,sa=address.street,ct=address.city,sa=address.state
 * with the above the mapping a row xyz is created.
 * the value patrickhenry is inserted into columnfamily address and column street of row xyz.
 * other values are inserted similarly.
 */
public class HBasePropPutOperator extends
		HBasePutOperator<String> {
	private static final Logger logger = LoggerFactory
			.getLogger(HBasePutOperatorTest.class);
	Properties prop;
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
		//return parseValues(prop.getProperty(t.getKey()), t.getValue());
	}
	
	
	public Put parseLine(String s)
	{
		Put put=null;
		HashMap<String,String> linemap=new HashMap<String, String>();
		String[] split=s.split("\\,");
		for(String pair:split)
		{
			linemap.put(pair.substring(0, pair.indexOf('=')), pair.substring(pair.indexOf('=')+1));
		}
		
		for (Entry<String,String> pair:linemap.entrySet()) {
			
			if(prop.getProperty(pair.getKey()).equals("row")){
				put=new Put(pair.getValue().getBytes());
				linemap.remove(pair.getKey());
				break;
			}
		}
		for (Entry<String,String> pair:linemap.entrySet()) {
			String propVal=prop.getProperty(pair.getKey());
			put.add(propVal.substring(0,propVal.indexOf('.')).getBytes(),propVal.substring(propVal.indexOf('.')+1).getBytes(),pair.getValue().getBytes());
		}
		return put;
	}
	public void loadFromHdfs() {
		prop = new Properties();
		FSDataInputStream input = null;
		FileSystem hdfs = null;
		try {
			Configuration conf=new Configuration();
			//this line may not be required while running in the cluster
			conf.set("fs.default.name","hdfs://localhost:8020/");
			hdfs = FileSystem.get(conf);
	
		    Path path = new Path(propFilepath);
			if (!hdfs.exists(path)) {
		        logger.error("property file does not exist");
		        return;
		    }
			input = hdfs.open(path);
			prop.load(input);
		} catch (IOException e) {

			e.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}

	public void loadProperty() {
		prop = new Properties();
		InputStream input = null;

		try {

			input = new FileInputStream("/home/cloudera/1.properties");
			prop.load(input);

		} catch (IOException ex) {
			ex.printStackTrace();
		} finally {
			if (input != null) {
				try {
					input.close();
				} catch (IOException e) {
					logger.error(e.getMessage());
				}
			}
		}
	}

	/**
	 * expects the property file to contain key which is the incoming tuple key.
	 * the value is of the format row.columnfamily.columnname
	 * @param valueStr
	 * @param value
	 * @return Put 
	 * 
	 */
	public Put parseValues(String valueStr, String value) {
		String[] splitStr = valueStr.split("\\.");
		Put put = new Put(splitStr[0].trim().getBytes());
		put.add(splitStr[1].trim().getBytes(), splitStr[2].trim().getBytes(),
				value.getBytes());
		return put;
	}

	 @Override
	public void setup(OperatorContext context)
	{
		 super.setup(context);
		// loadProperty();
			loadFromHdfs();
	}
	@Override
	public HBaseStatePersistenceStrategy getPersistenceStrategy() {
		return new HBaseRowStatePersistence();
	}

}
