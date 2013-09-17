/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 */

function DrawTopServerTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        topServerTable = new google.visualization.DataTable();
        topServerTable.addColumn('string', 'SERVER');
        topServerTable.addColumn('number', 'requests/sec');
        topServerTable.addRows(10);
        for(var i=0; (i <  pts.length)&&(i < 10); i++) 
        {
          var row = pts[i].split("##");
          topServerTable.setCell(i, 0, row[0]);
          topServerTable.setCell(i, 1, parseInt(row[1]));
          delete pts[i];
        }
        topServerTableChart.draw(topServerTable, {showRowNumber: true});
        delete topServerTable;
        delete data;
        delete pts;
      }
    }
    connect.open('GET',  "TopServer.php", true);
    connect.send(null);
  } catch(e) {
  }
}
