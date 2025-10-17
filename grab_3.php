<?php

$url = "https://zoek.officielebekendmakingen.nl/resultaten?svel=Publicatiedatum&svol=Aflopend&pg=50&q=(c.product-area==%22officielepublicaties%22)and(cql.textAndIndexes=%22preventief%22%20and%20cql.textAndIndexes=%22fouilleren%22)&zv=preventief%20fouilleren&col=&pagina=";

for ($i = 1; $i <= 26; $i++) {
    $page = file_get_contents($url . $i);
    file_put_contents("cache/page_3_" . $i . ".html", $page);
    echo "Grab: " . $i . "\n";
    sleep(1);
}