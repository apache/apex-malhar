/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
 */

function DrawClientDataTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        var clientDataTable = new google.visualization.DataTable();
        clientDataTable.addColumn('string', 'Client Data > 1K');
        for(var i=0; i <  (pts.length-1); i++) 
        {
          var row = new Array();
          row.push(pts[i]);
          clientDataTable.addRow(row);
        }
        document.getElementById('totaldata').innerHTML = pts[pts.length-1];
        clientDataTableChart.draw(clientDataTable, {showRowNumber: true});
        //document.getElementById('client_data_div').innerHTML = data;
        //delete clientDataTable;
        //delete data;
        //delete pts;
      }
    }
    connect.open('GET',  "/ClientData.php", true);
    connect.send(null);
  } catch(e) {
  }
}
