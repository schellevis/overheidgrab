<?php

require("simple_html_dom.php");
require("vendor/autoload.php");
require("config.php");

use Saturio\DuckDB\DuckDB;
use Saturio\DuckDB\Type\Type;

/**
 * Quote/sanitize SQL identifiers (table/column names).
 * - Replace all non [A-Za-z0-9_] chars with '_'
 * - If starts with a digit, prefix with 'c_'
 * - Quote with double quotes and escape inner quotes
 */
function ident(string $s): string {
    $s = preg_replace('/[^A-Za-z0-9_]/', '_', $s);
    if ($s === '' || $s === null) $s = 'col';
    if (preg_match('/^\d/', $s)) $s = 'c_' . $s;
    return '"' . str_replace('"', '""', $s) . '"';
}

$docs = [];
$hr   = ["size", "broodtekst","source","timestamp"]; // headers, start met size
$i = 1;

// Scan docs directory
$dir = scandir("docs");
$dir = array_diff($dir, [".", "..", ".DS_Store"]);

$count = count($dir);

foreach ($dir as $doc) {
    echo("Uitlezen: $i van $count $doc... ");
    $path = "docs/" . $doc;
    if (!is_file($path)) continue;
  //   if($i > 10) { continue; }

    // HTML inladen, defensief
    $html = @file_get_html($path);
    if (!$html) continue;

    $element = [];
    $element["id"] = uniqid();

    $meta = $html->find("meta") ?? [];
    foreach ($meta as $m) {
       
        $name = isset($m->name) ? trim($m->name) : '';
        if ($name === '' || strtolower($name) === 'viewport') continue;

        // Sanitize kolomnaam NU, zodat het overeenkomt met CREATE TABLE
        $safeName = preg_replace('/[^A-Za-z0-9_]/', '_', $name);
        if ($safeName === '' || $safeName === null) $safeName = 'col';
        if (preg_match('/^\d/', $safeName)) $safeName = 'c_' . $safeName;

        $hr[$safeName] = $safeName;

        $content = isset($m->content) ? $m->content : null;
        $element[$safeName] = $content;
        $element["size"] = round(filesize("docs/" . $doc) / 1024,0);  
        
        $broodtekst = $html->find("div[id=broodtekst]", 0)->plaintext  ?? '';
        $element["broodtekst"] = $broodtekst;
        $element["source"] = "officielebekendmakingen";
    }


    if (!empty($element)) {
        print_r($element);
        $docs[] = $element;
    }

    $i++;

    echo(" OK\n");
}

// --- Optioneel: debug ---
// print_r($docs);



// --- Optioneel: debug ---
// echo $query . PHP_EOL;

file_put_contents("docs.json", json_encode($docs, JSON_PRETTY_PRINT));
file_put_contents("headers.json", json_encode(array_values($hr), JSON_PRETTY_PRINT));