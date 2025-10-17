<?php

$url = "https://zoek.officielebekendmakingen.nl/resultaten?svel=Publicatiedatum&svol=Aflopend&pg=50&q=(c.product-area==%22officielepublicaties%22)and(cql.textAndIndexes=%22noodverordening%22)%20AND%20w.publicatienaam==%22Gemeenteblad%22&zv=risicogebied&col=&pagina=";

for ($i = 1; $i <= 73; $i++) {
    $page = file_get_contents($url . $i);
    file_put_contents("cache/page_risicogebied_" . $i . ".html", $page);
    echo "Grab: " . $i . "\n";
    sleep(1);
}