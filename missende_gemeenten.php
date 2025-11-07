<?php



require("simple_html_dom.php");
require("vendor/autoload.php");
require("config.php");

use Saturio\DuckDB\DuckDB;
use Saturio\DuckDB\Type\Type;


$db = DuckDB::create($database);

$gemeenten = file_get_contents("gemeenten.txt");
$gemeenten = explode("\n", $gemeenten);

foreach($gemeenten as $gemeente) {
    // $query = getData("SELECT * FROM gecombineerd WHERE gemeente='".$gemeente."' LIMIT 1");

    // $c = count($query);

    // if($c == 0) {
    //     echo $gemeente."\n";
    // }  
}


$unique = getData("SELECT DISTINCT gemeente FROM gecombineerd WHERE gemeente ILIKE '%bergen%'");
print_r($unique);