<?php
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
header("Content-type: application/json");
$redis = new Redis();
$redis->connect('127.0.0.1');
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
