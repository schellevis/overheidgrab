<?php

require("vendor/autoload.php");
require("config.php");


$dir = scandir("cache_google");
$dir = array_diff($dir, array('.', '..'));

$context = stream_context_create([
    'http' => [
        'timeout' => 20, // 20 seconden
    ]
]);

foreach($dir as $x=>$json) {
    $json = json_decode(file_get_contents("cache_google/".$json), true);



    foreach($json["items"] as $n=>$item) {
     $link =  $item["link"];
              $id = uniqid();
         $filename = hash("sha256", $link);

         echo("Downloaden $x-$n...");

         if(!file_exists("docs_google/".$filename)) {
             ("OK");
         } else {
             
             continue;
         }
         $data = $item;

        echo $filename;
     $item = @file_get_contents($link, false, $context);


     echo("\n");


     file_put_contents("docs_google/".$filename, $item);




    };
}


?>