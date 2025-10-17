<?php

require("vendor/autoload.php");
require("config.php");

use Saturio\DuckDB\DuckDB;
use Saturio\DuckDB\Type\Type;

$db = DuckDB::create($database);

$dir = scandir("cache_google");
$dir = array_diff($dir, array('.', '..'));

           $db->query("DROP TABLE IF EXISTS google_download");

            $db->query("
                CREATE TABLE google_download (
                    id TEXT PRIMARY KEY,
                    filename TEXT,
                    metadata TEXT,
                    fileformat TEXT,
                    mime TEXT
                )
            ");

$context = stream_context_create([
    'http' => [
        'timeout' => 20, // 20 seconden
    ]
]);

foreach($dir as $x=>$json) {
    $json = json_decode(file_get_contents("cache_google/".$json), true);



    foreach($json["items"] as $n=>$item) {
     $link =  $item["link"];
              $id = uniqid();
         $filename = hash("sha256", $link);

         echo("toevoegen $x-$n...");

 
         $data = $item;

        echo $filename;

 

     echo("\n");




     $query = "INSERT INTO google_download (id, filename, metadata, fileformat, mime) VALUES (?, ?, ?, ?, ?);";
        $stmt = $db->preparedStatement($query);
        $stmt->bindParam(1, $id);
        $stmt->bindParam(2,  $filename);
        $stmt->bindParam(3, json_encode($data));
        $stmt->bindParam(4, $data["fileFormat"] ?? "");
        $stmt->bindParam(5, $data["mime"] ?? "");
        $stmt->execute();
     


    };
}


?>