<?php



require("simple_html_dom.php");
require("vendor/autoload.php");
require("config.php");

use Saturio\DuckDB\DuckDB;
use Saturio\DuckDB\Type\Type;


$db = DuckDB::create($database);

$gemeenten = file_get_contents("gemeenten.txt");
$gemeenten = explode("\n", $gemeenten);

$wel = 0;
$niet = 0;

foreach($gemeenten as $gemeente) {
     $query = getData("SELECT * FROM gecombineerd 

     WHERE gemeente='".$gemeente."' LIMIT 1");
   $c = count($query);

   if($c == 0) { $niet++; }
   else { $wel++; }

         echo $gemeente.": $c\n";
     
}

echo "Wel: $wel\n";
echo "Niet: $niet\n";

exit;
$unique = getData("SELECT DISTINCT gemeente FROM gecombineerd WHERE gemeente ILIKE '%bergen%'");
print_r($unique);

//   LEFT JOIN koppeling ON koppeling.hash = gecombineerd.hash
//   LEFT JOIN flagging ON flagging.identifier = koppeling.identifier