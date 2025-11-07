<?php

require("simple_html_dom.php");
require("vendor/autoload.php");
require("config.php");

use Saturio\DuckDB\DuckDB;
use Saturio\DuckDB\Type\Type;

$db = DuckDB::create($database);

$data = getData("SELECT * FROM google_download");

// foreach($data as $i=>$row) {
//     $meta = json_decode($row['metadata'], true);



//     foreach($meta["pagemap"]["metatags"][0] as $key=>$value) {

//     $rows[$i][$key] = $value;
//     $keys[$key] = $key; }

    
// }



