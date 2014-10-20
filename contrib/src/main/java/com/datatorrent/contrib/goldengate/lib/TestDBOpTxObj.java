package com.datatorrent.contrib.goldengate.lib;

import java.io.IOException;
import java.text.SimpleDateFormat;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;

public class TestDBOpTxObj
{

  public static void main(String[] args) throws JsonGenerationException, JsonMappingException, IOException, ClassNotFoundException
  {
    String objS = "{\"readTime\":\"2014-09-11 20:11:01.000877\",\"size\":1,\"totalOps\":1,\"ggTranID\":{\"position\":\"00000000000000015270\",\"seqNo\":0,\"string\":\"SeqNo=0, RBA=15270\",\"rba\":15270},\"ops\":[{\"cols\":[{\"beforeValue\":\"\",\"afterValue\":\"34\",\"value\":null,\"changed\":true,\"missing\":false},{\"beforeValue\":\"\",\"afterValue\":\"dtuser34\",\"value\":null,\"changed\":true,\"missing\":false}],\"numCols\":2,\"operationType\":\"DO_INSERT\",\"position\":\"00000000000000015270\",\"positionRba\":15270,\"positionSeqno\":0,\"sqlType\":\"INSERT\",\"tableName\":{\"fullName\":\"OGGUSER.EMPLOYEE\",\"schemaName\":\"OGGUSER\",\"shortName\":\"EMPLOYEE\",\"originalName\":\"OGGUSER.EMPLOYEE\",\"originalShortName\":\"EMPLOYEE\",\"originalSchemaName\":\"OGGUSER\"},\"txState\":\"WHOLE\"}]}";
    ObjectMapper mapper = new ObjectMapper();
    mapper.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSSSSS"));
    _DsTransaction _dt = mapper.readValue(objS, _DsTransaction.class);
    System.out.println(_dt);
  }

}
