<?php



$i = 0;

while($i < 999999) {
    $url = "https://api.openraadsinformatie.nl/v1/elastic/*/_search?q=artikel+151b+gemeentewet&size=100";

    if($i > 1) {
        $url = $url."&from=".$i;


    }


    $json = file_get_contents($url);
    file_put_contents("cache_raadsinfo/raadsinfo_artikel151b_".$i.".json", $json);
    print_r(json_decode($json, true));
        echo $url."\n";
        $i = $i + 100;
        sleep(1);
}

