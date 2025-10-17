<?php

$dir = scandir("cache_raadsinfo");
$dir = array_diff($dir, array('.', '..'));

foreach($dir as $file) {
    $json = json_decode(file_get_contents("cache_raadsinfo/".$file), true);
$hr = [];
foreach($json["hits"]["hits"] as $hit) {
    
    foreach($hit as $key=>$hit) {
        $key = str_replace("_","",$key);
        $hr[$key] = $key;
    }

   foreach($hit["_source"] as $key=>$doc) {
    $key = str_replace("_","",$key);
    $hr[$key] = $key;
  //  print_r($doc);
   }
}
}

print_r($hr);