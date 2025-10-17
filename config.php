<?php

$database = "azc.db";

function getData($query) {
    global $db;
    $result = $db->query($query);
    $data = [];
    
    $i = 1;
    foreach ($result->rows(columnNameAsKey: true) as $row) {
    foreach ($row as $column => $value) {
            $data[$i][$column] = $value;
    }
    $i++;
}

return $data;
}