<?php

$i = 0;

$term = "risicogebied";

echo $term;

while($i < 400) { 
$url = "https://open.overheid.nl/overheid/openbaarmakingen/api/v0/zoek?zoektekst=$term&start=$i&aantalResultaten=50";

    echo $url;
        $page = file_get_contents($url);
    file_put_contents("cache_openoverheid/page_".$term."_" . $i . ".json", $page);
    echo "Grab: " . $i . "\n";
    sleep(1);

    $i = $i + 50;
}

$i = 0;

$term = "noodverordening";
echo $term;


while($i < 400) { 
$url = "https://open.overheid.nl/overheid/openbaarmakingen/api/v0/zoek?zoektekst=$term&start=$i&aantalResultaten=50";

    echo $url;
        $page = file_get_contents($url);
    file_put_contents("cache_openoverheid/page_".$term."_" . $i . ".json", $page);
    echo "Grab: " . $i . "\n";
    sleep(1);

    $i = $i + 50;
}

$i = 0;

$term = "veiligheidsrisicogebied";
echo $term;

while($i < 400) { 
$url = "https://open.overheid.nl/overheid/openbaarmakingen/api/v0/zoek?zoektekst=$term&start=$i&aantalResultaten=50";

    echo $url;
        $page = file_get_contents($url);
    file_put_contents("cache_openoverheid/page_".$term."_" . $i . ".json", $page);
    echo "Grab: " . $i . "\n";
    sleep(1);

    $i = $i + 50;
}