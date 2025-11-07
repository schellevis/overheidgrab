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

$data = getData("SELECT id,hash FROM gecombineerd ORDER BY ts asc");

foreach($data as $gc) {
    $o = $gc['id'];
    $hash = $gc['hash'];

    $parsed = str_replace("https://open.overheid.nl/documenten/","",$o);
    $parsed = str_replace("https://zoek.officielebekendmakingen.nl/","",$parsed);
    $parsed = str_replace(".html","",$parsed);

    $rows[$parsed][$hash]["orig"] = $o;
}

$i = 1;

foreach($rows as $id=>$row) {
    


    foreach($row as $hash=>$r) {


        $orig = $r["orig"];

        $sql = "INSERT INTO koppeling (hash,orig_id,identifier,incident) VALUES (
            '".$hash."',
            '".$orig."',
            '".$id."',
            '".$i."'
        )";

        $db->query($sql);


    }
    
    $i++;
}
