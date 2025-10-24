<?php

require("simple_html_dom.php");
require("vendor/autoload.php");
require("config.php");
use Saturio\DuckDB\DuckDB;
use Saturio\DuckDB\Type\Type;
$db = DuckDB::create($database);

$all = scandir("cache_openoverheid");
$all = array_diff($all, [".", "..", ".DS_Store"]);

$publicaties = [];
        //  $db->query("DROP TABLE IF EXISTS pdf_text");
           $db->query("DROP TABLE IF EXISTS openoverheid_extrameta");
            $db->query("DROP TABLE IF EXISTS openoverheid_text_doc");
              $db->query("DROP TABLE IF EXISTS openoverheid_text_pages");
                       $db->query("DROP TABLE IF EXISTS openoverheid_meta");
                       $db->query("DROP TABLE IF EXISTS openoverheid_files");

            $db->query("
                CREATE TABLE openoverheid_extrameta (
                    uniqid TEXT PRIMARY KEY,
                    id TEXT,
                    filename TEXT,
                    pid TEXT,
                    titel TEXT,
                    omschrijving TEXT,
                    weblocatie TEXT,
                    openbaarmakingsdatum timestamp,
                    publisher TEXT,
                    aanbieder TEXT,
                    mutatiedatumtijd timestamp,
                    bestandsgrootte TEXT,
                    aantalpaginas INT,
                    bestandstype TEXT
                )
            ");




foreach($all as $file) {
 $json = json_decode(file_get_contents("cache_openoverheid/".$file), true);






    foreach($json["resultaten"] as $pub) {
        if(isset($pub["document"]["pid"])) {
        $pid = $pub["document"]["pid"];
        $id = $pub["document"]["id"];

        $bestandsnaam = "docs_openoverheid/".$id.".pdf";

            $gmb = $pub["document"]["weblocatie"] ?? null;
            if(isset($gmb)) {
        $gmb = str_replace("https://zoek.officielebekendmakingen.nl/", "", $gmb);

        
            $query = "SELECT 1 FROM gecombineerd WHERE id = '".$gmb."' LIMIT 1";
            $check = getData($query);
            print_r($check);
            }

           

        $uniqid = uniqid();

        $query = "INSERT INTO openoverheid_extrameta (filename, id, pid, titel, omschrijving, openbaarmakingsdatum, publisher, aanbieder, mutatiedatumtijd, bestandsgrootte, aantalpaginas, bestandstype, uniqid, weblocatie) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
                        $stmt = $db->preparedStatement($query);
                        $stmt->bindParam(1, $bestandsnaam);
                        $stmt->bindParam(2, $id);
                        $stmt->bindParam(3, $pid);
                        $stmt->bindParam(4, $pub["document"]["titel"]);
                        $stmt->bindParam(5, $pub["document"]["omschrijving"] ? $pub["document"]["omschrijving"] : null);
                        $stmt->bindParam(6, isset($pub["document"]["openbaarmakingsdatum"]) ? date("Y-m-d H:i:s", strtotime($pub["document"]["openbaarmakingsdatum"])) : null);
                        $stmt->bindParam(7, $pub["document"]["publisher"]);
                        $stmt->bindParam(8, $pub["document"]["aanbieder"]);
                        $stmt->bindParam(9, isset($pub["document"]["mutatiedatumtijd"]) ? date("Y-m-d H:i:s", strtotime($pub["document"]["mutatiedatumtijd"])) : null);
                        $stmt->bindParam(10, isset($pub["bestandsgrootte"]) ? $pub["bestandsgrootte"] : null);
                        $stmt->bindParam(11, isset($pub["aantalpaginas"]) ? $pub["aantalpaginas"] : null);
                        $stmt->bindParam(12, isset($pub["bestandsType"]) ? $pub["bestandsType"] : null);
                        $stmt->bindParam(13, $uniqid);
                        $stmt->bindParam(14, $pub["document"]["weblocatie"] ? $pub["document"]["weblocatie"] : null);
                        $stmt->execute();
  
            if(file_exists("docs_openoverheid/".$id.".pdf")) {
                
            } else {
               
                
                }
        }
    }
}

