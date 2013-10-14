package com.datatorrent.demos.mroperator;

import org.apache.hadoop.mapred.Counters.Counter;
import org.apache.hadoop.mapred.Counters;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.Reporter;

public class ReporterImpl implements Reporter {
	
	private Counters counters;
	InputSplit inputSplit;
	
	public enum ReporterType {
		Mapper, Reducer
	}
	
	private ReporterType typ;
	
	public ReporterImpl(final ReporterType kind, final Counters ctrs) {
		this.typ = kind;
		this.counters = ctrs;
	}
	
	@Override
	public InputSplit getInputSplit() {
		if (typ == ReporterType.Reducer) {
			throw new UnsupportedOperationException("Reducer cannot call getInputSplit()");
		} else {
			return inputSplit;
		}
	}
	
	public void setInputSplit(InputSplit inputSplit) {
		this.inputSplit = inputSplit;
	}
	
	@Override
	public void incrCounter(Enum<?> key, long amount) {
		if (null != counters) {
			counters.incrCounter(key, amount);
		}
	}
	
	@Override
	public void incrCounter(String group, String counter, long amount) {
		if (null != counters) {
			counters.incrCounter(group, counter, amount);
		}
	}
	
	@Override
	public void setStatus(String status) {
		// do nothing.
	}
	
	@Override
	public void progress() {
		// do nothing.
	}
	
	@Override
	public Counter getCounter(String group, String name) {
		Counters.Counter counter = null;
		if (counters != null) {
			counter = counters.findCounter(group, name);
		}
		
		return counter;
	}
	
	@Override
	public Counter getCounter(Enum<?> key) {
		Counters.Counter counter = null;
		if (counters != null) {
			counter = counters.findCounter(key);
		}
		
		return counter;
	}
	
	public float getProgress() {
		return 0;
	}
	
}
