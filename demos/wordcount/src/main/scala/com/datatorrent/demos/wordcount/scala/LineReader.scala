package com.datatorrent.demos.wordcount.scala

import java.io.{BufferedReader, InputStream, InputStreamReader}

import com.datatorrent.api.DefaultOutputPort
import com.datatorrent.lib.io.fs.AbstractFileInputOperator
import org.apache.hadoop.fs.Path

class LineReader extends AbstractFileInputOperator[String] {
   @transient
   private var br : BufferedReader = null
   @transient
   val out : DefaultOutputPort[String] = new DefaultOutputPort[String]();

   override def readEntity(): String = br.readLine()

   override def emit(line: String): Unit = out.emit(line)

   override def openFile(path: Path): InputStream = {
     val in = super.openFile(path)
     br = new BufferedReader(new InputStreamReader(in))
     return in
   }

   override def closeFile(is: InputStream): Unit = {
     if (br != null) br.close()
     super.closeFile(is)
   }
 }
