<?php

require("simple_html_dom.php");

$all = scandir("cache");
$all = array_diff($all, [".", "..", ".DS_Store"]);

$publicaties = [];

foreach($all as $file) {
    $html = file_get_html("cache/".$file);
    $ret = $html->find("div[id=Publicaties]", 0 );
    $ul = $ret->find("li");
    
    foreach($ul as $li) {
       foreach($li->find("h2[class=result--title]") as $a) { 
        foreach($a->find("a") as $link) {
            $title = trim($link->plaintext);
            $url = "https://zoek.officielebekendmakingen.nl/" . $link->href;
            $publicaties[] = [
                "title" => $title,
                "url" => $url,
                "pretty" => $link->href
            ];
            $sum = md5($title);
            $links[] = $url;
        }
       }

    }
}

$links = array_unique($links);

file_put_contents("all.json", json_encode($publicaties, JSON_PRETTY_PRINT));