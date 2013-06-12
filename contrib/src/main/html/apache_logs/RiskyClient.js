/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
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
        riskyCleintTable.addColumn('string', 'Client Access More 5 Urls/sec');
        for(var i=0; i <  pts.length; i++) 
        {
          var row = new Array();
          row.push(pts[i]);
          riskyCleintTable.addRow(row);
        }
        //document.getElementById('risky_client_div').innerHTML = data;
        //document.getElementById('risky_client_div').innerHTML = riskyCleintTable.getNumberOfRows();
        riskyClientTableChart.draw(riskyCleintTable, {showRowNumber: true});
        delete riskyCleintTable;
        delete data;
        delete pts;
      }
    }
    connect.open('GET',  "/RiskyClient.php", true);
    connect.send(null);
  } catch(e) {
  }
}
