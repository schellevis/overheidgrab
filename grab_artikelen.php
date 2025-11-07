<?php

$url = "https://zoek.officielebekendmakingen.nl/resultaten?q=(c.product-area==%22officielepublicaties%22)and(cql.textAndIndexes=%22artikel%22%20and%20cql.textAndIndexes=%22151b%22%20and%20cql.textAndIndexes=%22gemeentewet%22)%20AND%20w.publicatienaam==%22Gemeenteblad%22&zv=artikel+151b+gemeentewet&pg=50&col=&svel=Publicatiedatum&svol=Aflopend&sf=ds%7cGemeenteblad";

for ($i = 1; $i <= 43; $i++) {
    $page = file_get_contents($url . $i);
    file_put_contents("cache/artikel_151b_" . $i . ".html", $page);
    echo "Grab: " . $i . "\n";
    sleep(1);
}


$url = "https://zoek.officielebekendmakingen.nl/resultaten?q=(c.product-area==%22officielepublicaties%22)and(cql.textAndIndexes=%22artikel%22%20and%20cql.textAndIndexes=%22176%22%20and%20cql.textAndIndexes=%22gemeentewet%22)%20AND%20w.publicatienaam==%22Gemeenteblad%22&zv=artikel+176+gemeentewet&pg=50&col=&svel=Publicatiedatum&svol=Aflopend&sf=ds%7cGemeenteblad";

for ($i = 1; $i <= 85; $i++) {
    $page = file_get_contents($url . $i);
    file_put_contents("cache/artikel_176_" . $i . ".html", $page);
    echo "Grab: " . $i . "\n";
    sleep(1);
}