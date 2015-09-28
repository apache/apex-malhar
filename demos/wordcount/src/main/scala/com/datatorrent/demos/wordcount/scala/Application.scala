package com.datatorrent.demos.wordcount.scala

import com.datatorrent.api.annotation.ApplicationAnnotation
import com.datatorrent.api.{DAG, DefaultInputPort, DefaultOutputPort, StreamingApplication}
import com.datatorrent.common.util.BaseOperator
import com.datatorrent.lib.algo.UniqueCounter
import com.datatorrent.lib.io.ConsoleOutputOperator
import org.apache.hadoop.conf.Configuration
import scala.beans.BeanProperty

class Parser extends BaseOperator {
   @BeanProperty
   var regex : String = " "

   @transient
   val out = new DefaultOutputPort[String]()

   @transient
   val in = new DefaultInputPort[String]() {
     override def process(t: String): Unit = {
       for(w <- t.split(regex)) out.emit(w)
     }
   }
 }

@ApplicationAnnotation(name="WordCountScala")
class Application extends StreamingApplication {
   override def populateDAG(dag: DAG, configuration: Configuration): Unit = {
     val input = dag.addOperator("input", new LineReader)
     val parser = dag.addOperator("parser", new Parser)
     val counter = dag.addOperator("counter", new UniqueCounter[String])
     val out = dag.addOperator("console", new ConsoleOutputOperator)

     dag.addStream[String]("lines", input.out, parser.in)
     dag.addStream[String]("words", parser.out, counter.data)
     dag.addStream[java.util.HashMap[String,Integer]]("counts", counter.count, out.input)
   }
 }
