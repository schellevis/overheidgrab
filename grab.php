<?php

$url = "https://zoek.officielebekendmakingen.nl/resultaten?svel=Publicatiedatum&svol=Aflopend&pg=50&q=(c.product-area==%22officielepublicaties%22)and(cql.textAndIndexes=%22veiligheidsrisicogebied%22)%20AND%20w.publicatienaam==%22Gemeenteblad%22&zv=veiligheidsrisicogebied&col=&pagina=";

for ($i = 1; $i <= 5; $i++) {
    $page = file_get_contents($url . $i);
    file_put_contents("cache/page_veiligheidsrisico" . $i . ".html", $page);
    echo "Grab: " . $i . "\n";
    sleep(1);
}


$url = "https://zoek.officielebekendmakingen.nl/resultaten?q=(c.product-area==%22officielepublicaties%22)and(cql.textAndIndexes=%22noodverordening%22)&pg=50&svel=Publicatiedatum&svol=Aflopend&zv=noodverordening&col=";

for ($i = 1; $i <= 5; $i++) {
    $page = file_get_contents($url . $i);
    file_put_contents("cache/page_nood" . $i . ".html", $page);
    echo "Grab: " . $i . "\n";
    sleep(1);
}




$url = "https://zoek.officielebekendmakingen.nl/resultaten?q=(c.product-area==%22officielepublicaties%22)and(cql.textAndIndexes=%22risicogebied%22)&pg=50&svel=Publicatiedatum&svol=Aflopend&zv=risicogebied&col=";

for ($i = 1; $i <= 5; $i++) {
    $page = file_get_contents($url . $i);
    file_put_contents("cache/page_risico" . $i . ".html", $page);
    echo "Grab: " . $i . "\n";
    sleep(1);
}



