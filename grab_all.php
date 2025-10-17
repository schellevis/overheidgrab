<?php

$all = json_decode(file_get_contents("all.json"), true);

ksort($all);



foreach($all as $link) {
    if(file_exists("docs/" . $link["pretty"])) {
        echo("Bestand bestaat al: " . $link["pretty"] . "\n");
        continue;
    }
    $url = $link["url"];
    echo("Downloaden: $url... ");
    $file = file_get_contents($url);
    file_put_contents("docs/" . $link["pretty"], $file);


    echo("OK\n");
//        sleep(1);
}
