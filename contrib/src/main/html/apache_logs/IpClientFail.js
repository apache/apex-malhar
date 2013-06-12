/*
 *  Copyright (c) 2012-2013 Malhar, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
 * @author Dinesh Prasad (dinesh@malhar-inc.com) 
 */

function DrawIpClientFailTableChart()
{
  try
  {
    var connect = new XMLHttpRequest();
    connect.onreadystatechange = function() {
      if(connect.readyState==4 && connect.status==200) {
        var data = connect.response;
        var pts = JSON.parse(data);
        var iptable = new google.visualization.DataTable();
        iptable.addColumn('string', 'Ip Client 404 Status');
        for(var i=0; i <  pts.length; i++) 
        {
          var row = new Array();
          row.push(pts[i]);
          iptable.addRow(row);
        }
        //document.getElementById('ipclient_404_div').innerHTML = data;
        IpClient404TableChart.draw(iptable, {showRowNumber: true});
        delete iptable;
        delete data;
        delete pts;
      }
    }
    connect.open('GET',  "/IpClient404.php", true);
    connect.send(null);
  } catch(e) {
  }
}
