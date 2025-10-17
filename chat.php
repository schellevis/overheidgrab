<?php

require("vendor/autoload.php");
require("config.php");
use Saturio\DuckDB\DuckDB;
use Saturio\DuckDB\Type\Type;

$db = DuckDB::create($database);



$results = getData("SELECT OVERHEIDop_publicationIssue as publicatie,DCTERMS_available as gepubliceerd,OVERHEIDop_jaargang as jaar, DCTERMS_publisher as gemeente, DC_title as titel,
   OVERHEIDop_gebiedsmarkering as gebied,DC_type as type,OVERHEID_category as categorie,DC_identifier as identifier, size
   FROM documenten
  where (titel ilike '%noodverordening' or (titel ilike '%risico%' and titel ilike '%veiligheid%' and titel ilike '%gebied%'))
  and titel not ilike '%intrekken%' and titel not ilike '%intrekking%';");


foreach($results as $r) {
    $doc = file_get_contents("docs/".$r["identifier"].".html");
    print_r($doc);
}