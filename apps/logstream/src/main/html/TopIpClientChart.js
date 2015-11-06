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
 * Functions fro charting top IpClient table.
 */

function DrawTopIpClientTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        topIpClientTable = new google.visualization.DataTable();
        topIpClientTable.addColumn('string', 'Client IP');
        topIpClientTable.addColumn('number', 'requests/sec');
        topIpClientTable.addRows(10);
        for(var i=0; (i <  pts.length)&&(i < 10); i++) 
        {
          var row = pts[i].split("##");
          topIpClientTable.setCell(i, 0, row[0]);
          topIpClientTable.setCell(i, 1, parseInt(row[1]));
          delete row
          delete pts[i];
        }
        topIpClientTableChart.draw(topIpClientTable, {showRowNumber: true});
        delete topIpClientTable;
        delete data;
        delete pts;
        //document.getElementById('top_IpClient_div').innerHTML = data;
      }
    }
    connect.open('GET',  "TopIpClientData.php", true);
    connect.send(null);
  } catch(e) {
  }
}
