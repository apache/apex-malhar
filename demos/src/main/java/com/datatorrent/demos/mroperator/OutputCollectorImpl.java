package com.datatorrent.demos.mroperator;

import java.io.IOException;
import java.io.PipedInputStream;
import java.io.PipedOutputStream;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.serializer.Deserializer;
import org.apache.hadoop.io.serializer.SerializationFactory;
import org.apache.hadoop.io.serializer.Serializer;
import org.apache.hadoop.mapred.OutputCollector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.lib.util.KeyHashValPair;

@SuppressWarnings("unchecked")
public class OutputCollectorImpl<K extends Object, V extends Object> implements OutputCollector<K, V> {
	private static final Logger logger = LoggerFactory.getLogger(OutputCollectorImpl.class);
	
	private List<KeyHashValPair<K, V>> list = new ArrayList<KeyHashValPair<K, V>>();
	
	public List<KeyHashValPair<K, V>> getList() {
		return list;
	}
	
	private transient SerializationFactory serializationFactory;
	private transient Configuration conf = null;
	
	public OutputCollectorImpl() {
		conf = new Configuration();
		serializationFactory = new SerializationFactory(conf);
		
	}
	
	private <T> T cloneObj(T t) throws IOException {
		Serializer<T> keySerializer;
		Class<T> keyClass;
		PipedInputStream pis = new PipedInputStream();
		PipedOutputStream pos = new PipedOutputStream(pis);
		keyClass = (Class<T>) t.getClass();
		keySerializer = serializationFactory.getSerializer(keyClass);
		keySerializer.open(pos);
		keySerializer.serialize(t);
		Deserializer<T> keyDesiralizer = serializationFactory.getDeserializer(keyClass);
		keyDesiralizer.open(pis);
		T clonedArg0 = keyDesiralizer.deserialize(null);
		pos.close();
		pis.close();
		keySerializer.close();
		keyDesiralizer.close();
		return clonedArg0;
		
	}
	
	@Override
	public void collect(K arg0, V arg1) throws IOException {
		if (conf == null) {
			conf = new Configuration();
			serializationFactory = new SerializationFactory(conf);
		}
		list.add(new KeyHashValPair<K, V>(cloneObj(arg0), cloneObj(arg1)));
	}
}
