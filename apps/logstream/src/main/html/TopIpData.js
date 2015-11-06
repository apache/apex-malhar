/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/**
 * Functions fro charting top url table.
 */

function DrawRiskyClientTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        var riskyCleintTable = new google.visualization.DataTable();
        riskyCleintTable.addColumn('string', 'Client Ip');
        riskyCleintTable.addColumn('number', 'bytes/sec');
        riskyCleintTable.addRows(10);
        for(var i=0; (i <  pts.length)&&(i < 10); i++) 
        {
          var row = pts[i].split("##");
          riskyCleintTable.setCell(i, 0, row[0]);
          riskyCleintTable.setCell(i, 1, parseInt(row[1]));
        }
        //document.getElementById('risky_client_div').innerHTML = data;
        //document.getElementById('risky_client_div').innerHTML = riskyCleintTable.getNumberOfRows();
        riskyClientTableChart.draw(riskyCleintTable, {showRowNumber: true});
        delete riskyCleintTable;
        delete data;
        delete pts;
      }
    }
    connect.open('GET',  "TopIpData.php", true);
    connect.send(null);
  } catch(e) {
  }
}
