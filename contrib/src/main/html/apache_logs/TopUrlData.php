<?php
header("Content-type: application/json");
$redis = new Redis();
//$redis->connect('127.0.0.1');
$redis->connect('node5.morado.com');
$redis->select(2);

// result array
$result = array();

for($i = 0; $i < 20; $i++)
{
  $value = $redis->get($i);
  //var_dump($value);
  $result[] = $value;
}

print json_encode($result);

?>
