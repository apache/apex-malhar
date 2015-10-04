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
$redis->select(1);
$format = 'YmdHi';
$incr = 60;

// Get from date 
$from = $_GET['from'];
if (!$from || empty($from)) {
  $from  = time()-3600;
}

// get url   
$url = $_GET['url'];

// result array
$result = array();

while ($from < time()) 
{
  $date = gmdate($format, $from);
  if (!$url || empty($url))
  {
    // view total   
    $total = 0;
        
    // home.php views
    $key =  'm|' . $date . '|0:mydomain.com/home.php';
    $arr =  $redis->hGetAll($key);
    $total += $arr[1];
            
    // contactus.php views
    $key =  'm|' . $date . '|0:mydomain.com/contactus.php';
    $arr =  $redis->hGetAll($key);
    $total += $arr[1];
    
    // contactus.php views
    $key =  'm|' . $date . '|0:mydomain.com/about.php';
    $arr =  $redis->hGetAll($key);
    $total += $arr[1];
    
    // contactus.php views
    $key =  'm|' . $date . '|0:mydomain.com/support.php';
    $arr =  $redis->hGetAll($key);
    $total += $arr[1];
    
    // products.php 
    for ($i = 0; $i < 100; $i++)
    {      
        $key =  'm|' . $date . '|0:mydomain.com/products.php?productid='. $i;
        $arr =  $redis->hGetAll($key);
        $total += $arr[1];
    }

    // services.php 
    for ($i = 0; $i < 100; $i++)
    {      
        $key =  'm|' . $date . '|0:mydomain.com/services.php?serviceid='. $i;
        $arr =  $redis->hGetAll($key);
        $total += $arr[1];
    }

    // partners.php 
    for ($i = 0; $i < 100; $i++)
    {      
        $key =  'm|' . $date . '|0:mydomain.com/partners.php?partnerid='. $i;
        $arr =  $redis->hGetAll($key);
        $total += $arr[1];
    }

    // store result in array   
    $result[] = array("timestamp" => $from * 1000, "url" => "all", "view" => $total);

  } else {
    
    $key =  'm|' . $date . '|0:' . $url;
    $arr = $redis->hGetAll($key);
    if ($arr)
    {
      $result[] = array("timestamp" => $from * 1000, "url" => $url, "view" => $arr[1]);
    }
  }
  $from += $incr;
}

array_pop($result);
print json_encode($result);

?>
