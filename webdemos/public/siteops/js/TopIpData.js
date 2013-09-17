/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
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
