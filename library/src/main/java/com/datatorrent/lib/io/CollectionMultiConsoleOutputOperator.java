package com.datatorrent.lib.io;

import java.util.Collection;
import java.util.Iterator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datatorrent.api.BaseOperator;
import com.datatorrent.api.DefaultInputPort;

/**
 * 
 * Writes tuples to standard out of the container
 * <p>
 * This is for specific use case for collection where I want to print each key value
 * pair in different line <br>
 * Mainly to be used for debugging. Users should be careful to not have this
 * node listen to a high throughput stream<br>
 * <br>
 * 
 * @since 0.3.4
 */

public class CollectionMultiConsoleOutputOperator<E> extends BaseOperator {
	private boolean debug = false;

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug = debug;
	}

	private static final Logger logger = LoggerFactory
			.getLogger(CollectionMultiConsoleOutputOperator.class);
	public final transient DefaultInputPort<Collection<E>> input = new DefaultInputPort<Collection<E>>() {
		@Override
		public void process(Collection<E> t) {
						
			Iterator<E> itr = t.iterator();
			System.out.println("{");

			while (itr.hasNext()) {
				E obj = itr.next();
				if (!silent) {
					System.out.println(obj.toString());
				}
				if (debug)
					logger.info(obj.toString());

			}
			System.out.println("}");
		}

	};

	boolean silent = false;

	private String stringFormat;

	public String getStringFormat() {
		return stringFormat;
	}

	public void setStringFormat(String stringFormat) {
		this.stringFormat = stringFormat;
	}

}
