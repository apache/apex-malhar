/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
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
        topIpClientTable.addColumn('number', 'requests per sec');
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
