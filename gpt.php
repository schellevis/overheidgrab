<?php

require("vendor/autoload.php");
require("config.php");

use Saturio\DuckDB\DuckDB;
use Saturio\DuckDB\Type\Type;

$db = DuckDB::create($database);

$db->query("DROP TABLE IF EXISTS gpt");

$db->query("CREATE TABLE gpt (
    incident INT PRIMARY KEY,
    id TEXT,
    ts TIMESTAMP,
    gemeente TEXT,
    hash TEXT,
    response TEXT
)");


$sql = "  SELECT DISTINCT koppeling.incident
  FROM gecombineerd
  LEFT JOIN koppeling ON koppeling.hash = gecombineerd.hash
  LEFT JOIN flagging  ON flagging.identifier = koppeling.identifier
  WHERE flagging.relevant = 'ja'
  ORDER BY koppeling.incident";


$incidenten = getData($sql);

foreach($incidenten as $incident) {
    $nr = $incident["incident"];

    echo("Incident $nr\n");

    $sql = "SELECT gecombineerd.hash,gecombineerd.tekst,gecombineerd.id,gecombineerd.ts,gecombineerd.gemeente FROM koppeling
    LEFT JOIN gecombineerd ON koppeling.hash = gecombineerd.hash
    WHERE incident = '$nr' LIMIT 1000";



    $response = "test";

    $sql = ("INSERT INTO gpt (incident, id, ts, gemeente, hash, response) VALUES (?, ?, ?, ?, ?, ?)");
    $stmt = $db->preparedStatement($sql);
    $stmt->bindParam(1, $incident, Type::DUCKDB_TYPE_INTEGER);


}