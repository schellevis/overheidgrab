<?php
require("config.php");
require("vendor/autoload.php");
use Saturio\DuckDB\DuckDB;
use Saturio\DuckDB\Type\Type;

  $db = DuckDB::create($database);

$db->query("ALTER TABLE azc.main.raadsinfo ADD COLUMN IF NOT EXISTS gemeente VARCHAR(100);");


$q = getData("SELECT DISTINCT _index FROM raadsinfo ORDER BY _index;");

foreach($q as $g) {
    $index = $g["_index"];
        if (preg_match('/^(?:ori|osi|owi|ggm)_(?:fixed_)?([a-z0-9_\-]+)_\d+$/i', $index, $m)) {
    $naam = str_replace('_', ' ', $m[1]);
      $naam = ucwords(strtolower($naam));
    $db->query("UPDATE azc.main.raadsinfo SET gemeente = '".$naam."' WHERE _index = '".$index."';");

} else {
    echo("Faal: ".$index."\n");
    continue;
}
}