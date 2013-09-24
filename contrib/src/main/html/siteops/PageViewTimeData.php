<?php
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
