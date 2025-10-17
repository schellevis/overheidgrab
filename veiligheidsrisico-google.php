<?php

$term = "aanwijziging risicogebied";

        $query = "$term filetype:pdf";
        $apiKey = "AIzaSyCuX9xpdz3aqKQPPQDWKe0SJa5sYqxalfI";
        $searchEngineId = "e15b8f4dc57c34e81";
        

        $i = 1;

        while ($i <= 999999) {

            if(file_exists("cache_google/veiligheidsrisico_google_" . $term . $i . ".json")) {
                $i++;
                continue;
            }
    
            $url = "https://www.googleapis.com/customsearch/v1?q=" . urlencode($query) .
            "&cx=$searchEngineId&key=$apiKey&start=$i";

        $response = file_get_contents($url);
        echo $i." ";
        //print_r(json_decode($response, true));

        file_put_contents("cache_google/veiligheidsrisico_google_" . $term . $i . ".json", $response);
            $i++;
        }