/**
 * Copyright (c) 2012-2012 Malhar, Inc.
 * All rights reserved.
 *
 */

/**
 * <b>{@link com.malhartech.lib.io}</b> is a library input operators for write into streams and output operators for reading from streams. The I/O
 * operators interact with entities outside of DAG, and at times outside of Hadoop<p>
 * <br>
 * <br> The classes are<br>
 * <b>{@link com.malhartech.lib.io.AbstractActiveMQInputOperator}</b>: Abstract class for ActiveMQ Input adaptors<br>
 * <b>{@link com.malhartech.lib.io.AbstractActiveMQOutputOperator}</b>: Abstract class for ActiveMQ Output adaptors<br>
 * <b>{@link com.malhartech.lib.io.AbstractActiveMQSinglePortInputOperator}</b>: Abstract class for ActiveMQ Input adapters with one port (schema)<br>
 * <b>{@link com.malhartech.lib.io.AbstractActiveMQSinglePortOutputOperator}</b>: Abstract class for ActiveMQ Output adapters with one port (schema(<br>
 * <b>{@link com.malhartech.lib.io.AbstractHDFSInputOperator}</b>: Abstract class for HDFS Input adapters (reads from)<br>
 * <b>{@link com.malhartech.lib.io.ActiveMQBase}</b>: ActiveMQ base class for connection/session handling<br>
 * <b>{@link com.malhartech.lib.io.ActiveMQConsumerBase}</b>: Base class for an ActiveMQ consumer (Input adapter)<br>
 * <b>{@link com.malhartech.lib.io.ActiveMQProducerBase}</b>: Base class for ActiveMQ producer (output adapter)<br>
 * <b>{@link com.malhartech.lib.io.ConsoleOutputOperator}</b>: Writes tuples to stdout of the Hadoop container<br>
 * <b>{@link com.malhartech.lib.io.HdfsOutputOperator}</b>: Writes to HDFS<br>
 * <b>{@link com.malhartech.lib.io.HttpInputOperator}</b>: Reads from a HTTP connection (Input adapter)<br>
 * <b>{@link com.malhartech.lib.io.HttpOutputOperator}</b>: Writes (POST) to a HTTP server<br>
 * <br>
 * <br>
 */

package com.malhartech.lib.io;
