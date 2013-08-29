/*
 *  Copyright (c) 2012-2013 DataTorrent, Inc.
 *  All Rights Reserved.
 */

/**
 * Functions fro charting top url table.
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
        document.getElementById('totaldata').innerHTML = pts[0];
      }
    }
    connect.open('GET',  "ClientData.php", true);
    connect.send(null);
  } catch(e) {
  }
}
