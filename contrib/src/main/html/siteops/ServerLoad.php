<?php
header("Content-type: application/json");
$redis = new Redis();
//$redis->connect('127.0.0.1');
$redis->connect('node5.morado.com');
$redis->select(4);
$format = 'YmdHi';
$incr = 60;

// Get from date 
$from = $_GET['from'];
if (!$from || empty($from)) {
  $from  = time()-3600;
}

// get server   
$server = $_GET['server'];

// result array
$result = array();

while ($from < time()) 
{
  $date = gmdate($format, $from);
  if (!$server || empty($server) || ($server == "all"))
  {
    // total server load  
    $total = 0;
          
    // server loads 
    $key =  'm|' . $date . '|0:server0.mydomain.com:80';
    $arr = $redis->hGetAll($key);
    $total += $arr[1];
    $key =  'm|' . $date . '|0:server1.mydomain.com:80';
    $arr = $redis->hGetAll($key);
    $total += $arr[1];
    $key =  'm|' . $date . '|0:server2.mydomain.com:80';
    $arr = $redis->hGetAll($key);
    $total += $arr[1];
    $key =  'm|' . $date . '|0:server3.mydomain.com:80';
    $arr = $redis->hGetAll($key);
    $total += $arr[1];
    $key =  'm|' . $date . '|0:server4.mydomain.com:80';
    $arr = $redis->hGetAll($key);
    $total += $arr[1];
    $key =  'm|' . $date . '|0:server5.mydomain.com:80';
    $arr = $redis->hGetAll($key);
    $total += $arr[1];
    $key =  'm|' . $date . '|0:server6.mydomain.com:80';
    $arr = $redis->hGetAll($key);
    $total += $arr[1];
    $key =  'm|' . $date . '|0:server7.mydomain.com:80';
    $arr = $redis->hGetAll($key);
    $total += $arr[1];
    $key =  'm|' . $date . '|0:server8.mydomain.com:80';
    $arr = $redis->hGetAll($key);
    $total += $arr[1];
    $key =  'm|' . $date . '|0:server9.mydomain.com:80';
    $arr = $redis->hGetAll($key);
    $total += $arr[1];

    // add to result 

    // add to result 
    $result[] = array("timestamp" => $from * 1000, "server" => "all", "view" => $total);

  } else {
    
    $key =  'm|' . $date . '|0:' . $server;
    $arr = $redis->hGetAll($key);
    if ($arr)
    {
      $result[] = array("timestamp" => $from * 1000, "server" => $server, "view" => $arr[1]);
    }
  }
  $from += $incr;
}

array_pop($result);
print json_encode($result);


?>
