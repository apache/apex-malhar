<?php
header("Content-type: application/json");
$redis = new Redis();
//$redis->connect('127.0.0.1');
$redis->connect('node5.morado.com');
$redis->select(6);

// result array
$result = array();

for($i = 0; $i < 10; $i++)
{
  $value = $redis->get($i);
  $result[] = $value;
}

print json_encode($result);


?>
