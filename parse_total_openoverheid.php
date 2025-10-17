<?php

require("simple_html_dom.php");

$all = scandir("cache_openoverheid");
$all = array_diff($all, [".", "..", ".DS_Store"]);

$publicaties = [];

foreach($all as $file) {
 $json = json_decode(file_get_contents("cache_openoverheid/".$file), true);


    foreach($json["resultaten"] as $pub) {
        print_r($pub);
    }
}

$links = array_unique($links);

file_put_contents("all_openoverheid.json", json_encode($publicaties, JSON_PRETTY_PRINT));