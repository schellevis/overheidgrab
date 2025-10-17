<?php

        $query = "noodverordening";
        $apiKey = "AIzaSyCuX9xpdz3aqKQPPQDWKe0SJa5sYqxalfI";
        $searchEngineId = "e15b8f4dc57c34e81";

        $gemeenten = file_get_contents("gemeenten.csv");
        $gemeenten = explode("\n", $gemeenten);
    
        

        $i = 1;

      foreach($gemeenten as $gemeente) {

            $shortgemeente = str_replace(".nl","",$gemeente);
            $squery = $query." " . $shortgemeente;
      


            if(!file_exists("cache_google/veiligheidsrisicogebied_$shortgemeente.json")) {
    
            $url = "https://www.googleapis.com/customsearch/v1?q=" . urlencode($squery) .
                    "&cx=$searchEngineId&key=$apiKey&start=$i";

            echo $url."\n";
         

        $response = file_get_contents($url);
        echo $i." ";
        //print_r(json_decode($response, true));

    
        file_put_contents("cache_google/veiligheidsrisicogebied_$shortgemeente.json", $response);
            $i++;

            sleep(1);


        } }