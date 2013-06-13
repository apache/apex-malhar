/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top IpClient table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
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
        topIpClientTable.addColumn('string', 'TopIpClient');
        for(var i=0; i <  pts.length; i++) 
        {
          var row = new Array();
          row.push(pts[i]);
          topIpClientTable.addRow(row);
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
