<?php



$i = 0;

while($i < 99999) {
    $url = "https://api.openraadsinformatie.nl/v1/elastic/*/_search?q=risicogebied&size=100";

    if($i > 1) {
        $url = $url."&from=".$i;


    }


    $json = file_get_contents($url);

        $data = json_decode($json, true);

    // <<< HIER: check of er nog hits zijn >>>
    if (empty($data['hits']['hits'])) {
        echo "Geen hits (meer) voor 'risicogebied' bij offset $i, stoppen.\n";
        break; // verlaat deze while, gaat daarna naar de volgende zoekterm
    }


    file_put_contents("cache_raadsinfo/newraadsinfo_ddrisicogebied_".$i.".json", $json);
    print_r(json_decode($json, true));
        echo $url."\n";
        $i = $i + 100;
        sleep(1);
}



$i = 0;

while($i < 99999) {
    $url = "https://api.openraadsinformatie.nl/v1/elastic/*/_search?q=noodverordening&size=100";

    if($i > 1) {
        $url = $url."&from=".$i;


    }


    $json = file_get_contents($url);

        $data = json_decode($json, true);

    // <<< HIER: check of er nog hits zijn >>>
    if (empty($data['hits']['hits'])) {
        echo "Geen hits (meer) voor 'risicogebied' bij offset $i, stoppen.\n";
        break; // verlaat deze while, gaat daarna naar de volgende zoekterm
    }


    file_put_contents("cache_raadsinfo/newraadsinfo_ddnood_".$i.".json", $json);
    print_r(json_decode($json, true));
        echo $url."\n";
        $i = $i + 100;
        sleep(1);
}


$i = 0;

while($i < 99999) {
    $url = "https://api.openraadsinformatie.nl/v1/elastic/*/_search?q=veiligheidsrisicogebied&size=100";

    if($i > 1) {
        $url = $url."&from=".$i;


    }


    $json = file_get_contents($url);

        $data = json_decode($json, true);

    // <<< HIER: check of er nog hits zijn >>>
    if (empty($data['hits']['hits'])) {
        echo "Geen hits (meer) voor 'risicogebied' bij offset $i, stoppen.\n";
        break; // verlaat deze while, gaat daarna naar de volgende zoekterm
    }


    file_put_contents("cache_raadsinfo/newraadsinfo_ddvrisicogebied_".$i.".json", $json);
    print_r(json_decode($json, true));
        echo $url."\n";
        $i = $i + 100;
        sleep(1);
}