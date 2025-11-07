<?php

$i = 0;

$term = "artikel+151b+gemeentewet";

while($i < 6000) { 
$url = "https://open.overheid.nl/overheid/openbaarmakingen/api/v0/zoek?zoektekst=$term&start=$i&aantalResultaten=50";

    echo $url;
        $page = file_get_contents($url);
    file_put_contents("cache_openoverheid/page_".$term."_" . $i . ".json", $page);
    echo "Grab: " . $i . "\n";
    sleep(1);

    $i = $i + 50;
}