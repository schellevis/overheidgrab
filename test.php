<?php

$page = file_get_contents("https://zoek.officielebekendmakingen.nl/resultaten?q=(c.product-area%3d%3d%22officielepublicaties%22)and(cql.textAndIndexes%3d%22veiligheidsrisicogebied%22)&zv=veiligheidsrisicogebied&pg=50&col=&svel=Publicatiedatum&svol=Aflopend");

file_put_contents("test.html", $page);