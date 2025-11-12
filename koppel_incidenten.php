<?php

# stap 1: combineren
# stap 2: koppelen

require("simple_html_dom.php");
require("vendor/autoload.php");
require("config.php");

use Saturio\DuckDB\DuckDB;
use Saturio\DuckDB\Type\Type;

$db = DuckDB::create($database);

$sql = "DROP TABLE IF EXISTS koppeling;

CREATE TABLE koppeling (
    hash TEXT,
    orig_id TEXT,
    identifier TEXT,
    incident TEXT
)";

$db->query($sql);

$data = getData("SELECT id,hash,ts,gemeente FROM gecombineerd ORDER BY ts desc");

foreach($data as $gc) {
    $o = $gc['id'];
    $hash = $gc['hash'];

    $parsed = str_replace("https://open.overheid.nl/documenten/","",$o);
    $parsed = str_replace("https://zoek.officielebekendmakingen.nl/","",$parsed);
    $parsed = str_replace(".html","",$parsed);



    $gemeente = strtolower($gc['gemeente']);
    if(!isset($gemeente) || empty($gemeente)) {
      continue;
    }

    $gemeente = str_replace(" ","_",$gemeente);
    $gemeente = urlencode($gemeente);



    $time = new DateTimeImmutable($gc['ts']);
    if(!$time) {
        continue;
    }

        $tijd = $time->format("oW");
        

   $rows[$gemeente][$tijd][$parsed][$hash]["orig"] = $o;


//    echo $time->format("oW");
//    echo("\n");  

}

//print_r($rows);

$i = 1;



foreach($rows as $gemeente=>$rowg) {
    foreach($rowg as $tijd=> $rowt) {
     

       

        foreach($rowt as $id=>$rowh) {
            

            

        foreach($rowh as $hash=>$r) {


                 echo("INCIDENT $i: ");
                echo $gemeente." - ".$tijd." - ".$id."\n";
            

         $orig = $r["orig"];

        $sql = "INSERT INTO koppeling (hash,orig_id,identifier,incident) VALUES (
            '".$hash."',
            '".$orig."',
            '".$id."',
            '".$i."'
        )";




    $db->query($sql);
            

        } 


    }      $i++; }
}


// foreach($rows as $id=>$row) {
    
//   //  print_r($row);


//     foreach($row as $hash=>$r) {


//         $orig = $r["orig"];

//         $sql = "INSERT INTO koppeling (hash,orig_id,identifier,incident) VALUES (
//             '".$hash."',
//             '".$orig."',
//             '".$id."',
//             '".$i."'
//         )";

//        $db->query($sql);

//    //     echo $i." van ".count($rows)."\n";


//     }
    
//     $i++;
// }
