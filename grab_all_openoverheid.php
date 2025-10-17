<?php

require("simple_html_dom.php");

$all = scandir("cache_openoverheid");
$all = array_diff($all, [".", "..", ".DS_Store"]);

$publicaties = [];

foreach($all as $file) {
 $json = json_decode(file_get_contents("cache_openoverheid/".$file), true);


    foreach($json["resultaten"] as $pub) {
        if(isset($pub["document"]["pid"])) {
        $pid = $pub["document"]["pid"];
        $id = $pub["document"]["id"];
        if(file_exists("docs_openoverheid/".$id.".pdf")) {
            continue;
        }
        $get = file_get_contents($pid."/pdf");
        file_put_contents("docs_openoverheid/".$id.".pdf", $get);
        echo $id."\n";
        sleep(1);
        }
    }
}

