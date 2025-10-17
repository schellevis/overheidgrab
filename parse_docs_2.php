<?php

require("simple_html_dom.php");
require("vendor/autoload.php");
require("config.php");

use Saturio\DuckDB\DuckDB;
use Saturio\DuckDB\Type\Type;

function ident(string $s): string {
    $s = preg_replace('/[^A-Za-z0-9_]/', '_', $s);
    if ($s === '' || $s === null) $s = 'col';
    if (preg_match('/^\d/', $s)) $s = 'c_' . $s;
    return '"' . str_replace('"', '""', $s) . '"';
}

$docs = json_decode(file_get_contents("docs.json"), true);
$hr = json_decode(file_get_contents("headers.json"), true);

echo count($docs);


// Maak CREATE TABLE met alle verzamelde meta-keys
$query = "CREATE TABLE bekendmakingen (\n  id CHAR(30) PRIMARY KEY";
foreach ($hr as $h) {
    $query .= ",\n  " . ident($h) . " ";
    if ($h === 'size') {
        $query .= "INTEGER";
    } elseif ($h == "timestamp") {
        $query .= "TIMESTAMP";
    } else {
        $query .= "TEXT";
    }
}
$query .= "\n);";

// // Nieuwe database aanmaken (of bestaande weghalen)
// if (isset($database) && file_exists($database)) {
//     unlink($database);
// }

echo $query;

$db = DuckDB::create($database);
//$db->query("DROP TABLE documenten;");
 $db->query("DROP TABLE IF EXISTS bekendmakingen;");
$db->query($query);
$db->query("set default_collation = 'nocase';");

// Insert elke doc
foreach ($docs as $n=>$doc) {
    if(!is_array($doc)) continue; // skip size
    // id = 10 chars (schema gebruikt CHAR(10))

    if($doc["DCTERMS_available"] != "") {
    $time = date_create_from_format('Y-m-d', $doc["DCTERMS_available"]);
    if($time) {
    $doc["timestamp"] = $time->format('Y-m-d H:i:s');
} else { echo("Fout: geen timestamp"); echo $doc["DCTERMS_available"]; }
    }
//    print_r($doc);
    echo $n . " ";
    // Kolommen quoten met ident()
    $columnsQuoted = array_map('ident', array_keys($doc));

    // Plaatshouders in $1..$N vorm (saturio/duckdb-php)
    $paramCount = count($doc);
    $placeholders = [];
    for ($i = 1; $i <= $paramCount; $i++) {
        $placeholders[] = '$' . $i;
    }

    $sql = sprintf(
        'INSERT INTO bekendmakingen (%s) VALUES (%s)',
        implode(', ', $columnsQuoted),
        implode(', ', $placeholders)
    );

    // Belangrijk: prepare met $sql (niet $query)
    $stmt = $db->preparedStatement($sql);

    // Bind alle waarden met simpele typemapping
    $i = 1;
    foreach ($doc as $value) {
    
        // DuckDB types kiezen
        $type = Type::DUCKDB_TYPE_VARCHAR;
        if (is_int($value)) {
            $type = Type::DUCKDB_TYPE_INTEGER;
        } elseif (is_bool($value)) {
            $type = Type::DUCKDB_TYPE_BOOLEAN;
            $value = $value ? 1 : 0; // veilig als boolean->int
        } elseif (is_float($value)) {
            $type = Type::DUCKDB_TYPE_DOUBLE;
        } elseif (is_null($value)) {
            // null laten staan; VARCHAR is prima
        } else {
            // alles naar string
            $value = (string)$value;
        }

        $stmt->bindParam($i, $value, $type);
        $i++;
    }

    $stmt->execute();

    // --- Optioneel: debug ---
    // echo $sql . PHP_EOL;
}

echo "Klaar. " . count($docs) . " documenten ingevoegd." . PHP_EOL;