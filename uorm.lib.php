<?php

class uorm {
    /** @var $db mysqli_logger */

    private static $db;

    /** ID of current user
     * @var $user string */
    private static $userID;
	
    /** Debugging on? 
     * @var $debug bool */
    public static $debug = false;

    /** @var $affected_rows int */
    public static $affected_rows = 0;

    /** Speciális kulcsú táblák */
    private static $specials = array(
        "Table1" => "primaryKey1", //example
    );

    static public function init($db, $userID = "", $debug = false) {
        static::$db = $db;
        static::$userID = $userID;
        static::$debug = $debug;
    }

    /**
     * Instering log entries into centralized Log (used by import functions)
     * @global array $user
     * @param string $table Database table ID
     * @param string $id ID of row of data
     * @param string $name modified column(s)
     * @param type $rawdata Raw data to save as previous value
     * @return string ID of data
     */
    static public function saveMetadata($table, $id, $name, $rawdata) {
        static::$db->query("START TRANSACTION;");
        list($nextEventID) = static::$db->query("SELECT MAX(logEventID)+1 FROM Log")->fetch_row;
        static::$db->query("INSERT INTO Log SET "
                        . "logEventID='" . static::$db->real_escape_string($nextEventID) . "', "
                        . "opID='" . static::$db->real_escape_string(static::$userID) . "', "
                        . "opDate=NOW(), "
                        . "logTable='" . static::$db->real_escape_string($table) . "', "
                        . "logEvent='METADATA', "
                        . "logItemID='" . static::$db->real_escape_string($id) . "', "
                        . "logColumn='" . static::$db->real_escape_string($name) . "', "
                        . "logValue='" . static::$db->real_escape_string($rawdata) . "' ") or die(__FILE__ . " on line " . __LINE__ . ": " . static::$db->error);

        static::$db->query("COMMIT;");
        return $id;
    }

    /**
     * Provides the key column name for a table
     * @param string $table
     */
    static private function keyName($table) {
        if (static::$specials[$table] != "") {
            $key = static::$specials[$table];
        } else {
            $key = strtolower($table) . "ID";
        }
        return $key;
    }

    /**
     * Manipulatig database records with centralized logging of previous value
     * @global array $user
     * @param string $table Database table ID
     * @param string $task Task: INSERT, UPDATE, DELETE
     * @param array $new New values (array of modified columns)
     * @param ID $id ID of entity (optional), $new can contain it
     * @return type ID of manuipulated row 
     */
    static public function save($table, $task, $new, $id = "") {
        $key = static::keyName($table);
        if ($id == "" && isset($new[$key])) {
            $id = $new[$key];
        }

        $old = static::get($table, $id);

        static::$db->query("START TRANSACTION;");
        list($nextEventID) = static::$db->query("SELECT MAX(logEventID)+1 FROM Log")->fetch_row();

        switch ($task) {
            case "INSERT_UPDATE":
                $insert_update = true;
                
            case "INSERT":

                $query = "INSERT INTO `{$table}` SET ";
                $query_set = "";
                foreach ($new as $k => $v) {
                    //Language INDEPENDENT columns goes into the main table
                    if (!preg_match("/(?<col>[^_]*)_(?<lang>[A-Z][A-Z])/", $k, $matches)) {
                        $query_set .= "$k='" . static::$db->real_escape_string($v) . "', ";
                    }
                }
                $query_set .= " opID='".static::$userID."', opDate=NOW()"; //
                

                if($insert_update) {
                    $query .= $query_set . " ON DUPLICATE KEY UPDATE " .$query_set;
                } else {
                    $query .= "";
                }
                
                static::$db->query($query) or die(static::$db->last_error . "<br>$query<br>" . __FILE__ . ": " . __LINE__);
                static::$affected_rows = static::$db->affected_rows;
                
                // ha nem kaptunk id-t akkor megnézzük, hogy lett a beszurt értéke
                if (empty($id)) {
                    $id = static::$db->insert_id;
                }

                static::$db->query("INSERT INTO `Log` SET logEventID='{$nextEventID}', opID='".static::$userID."', opDate=NOW(), "
                                . "logTable='{$table}', logEvent='INSERT', logItemID='{$id}', logColumn='', logValue='" . static::$db->real_escape_string(serialize($new)) . "' ") or die(__FILE__ . " at line " . __LINE__ . ": " . static::$db->error);

                //Language DEPENDENT columns goes into tableLang
                foreach ($new as $k => $v) {
                    if (preg_match("/(?<col>[^_]*)_(?<lang>[A-Z][A-Z])/", $k, $matches)) {
                        $_set = "{$matches['col']} = '" . static::$db->real_escape_string($v) . "', opID='".static::$userID."', opDate=NOW()";
                        static::$db->query("INSERT INTO `{$table}Lang` SET {$_set}, {$key}='{$id}', langID='{$matches['lang']}'  ON DUPLICATE KEY UPDATE {$_set}") or die(__FILE__ . " at line " . __LINE__ . ": " . static::$db->error);
                    }
                }
                break;

            case "UPDATE":


                if ($id == "") {
                    print_r($new);
                    exit("Hiba: a frissítendő elem azonoisítója ismeretlen.");
                }
                $query = "UPDATE `$table` SET ";

                $c = 0;
                foreach ($new as $k => $v) {
                    
                    if ($k != $key && ($old[$k] != $v)) { // || $old[$k] == ""
                        $c++;
                        //Nyelvfüggő oszlopok külön táblába mennek
                        if (preg_match("/(?<col>[^_]*)_(?<lang>[A-Z][A-Z])/", $k, $matches)) {

                            $old_lang = static::$db->query("SELECT $key,{$matches['col']} FROM {$table}Lang WHERE {$key}='{$id}' AND langID='{$matches['lang']}'")->fetch_assoc();
                            if ($old_lang[$matches['col']] != $v) {
                                //Csak akkor logolunk, ha volt változás
                                static::$db->query("INSERT INTO Log SET logEventID='$nextEventID', opID='".static::$userID."', opDate=NOW(), "
                                                . "logTable='$table', logEvent='UPDATE', logItemID='$id', logColumn='$k', logValue='" . static::$db->real_escape_string($old_lang[$matches['col']]) . "' ") or die(__FILE__ . " on line " . __LINE__ . ": " . static::$db->error);
                            }
                            if ($old_lang[$key] != $id) {
                                static::$db->query("INSERT INTO {$table}Lang SET {$matches['col']} = '" . static::$db->real_escape_string($v) . "', opID='".static::$userID."', opDate=NOW(), {$key}='{$id}', langID='{$matches['lang']}'") or die(__FILE__ . " at line " . __LINE__ . ": " . static::$db->error);
                            } else {
                                static::$db->query("UPDATE {$table}Lang SET {$matches['col']} = '" . static::$db->real_escape_string($v) . "', opID='".static::$userID."', opDate=NOW() WHERE {$key}='{$id}' AND langID='{$matches['lang']}'") or die(__FILE__ . " at line " . __LINE__ . ": " . static::$db->error);
                            }
                        } else {
                            //Nyelv független oszlopok
                            $query .= "$k='" . static::$db->real_escape_string($v) . "', ";

                            if ($old[$k] != $v) {
                                //Csak a megváltozottakat logoljuk
                                static::$db->queryDie("INSERT INTO Log SET logEventID='$nextEventID', opID='".static::$userID."', opDate=NOW(), "
                                        . "logTable='$table', logEvent='UPDATE', logItemID='$id', logColumn='$k', logValue='" . static::$db->real_escape_string($old[$k]) . "' ");
                            }
                        }
                    }
                }
                //Csak akkor frissítünk ha történt is változás.
                if($c == 0) break;
                
                $query .= " opID='".static::$userID."', opDate=NOW() ";

                $query .= " WHERE $key='$id'";

                if (static::$debug) {
                    echo "<pre>$query</pre>";
                }

                static::$db->query($query) or die(static::$db->last_error . "\nQUERY: $query" . print_r($new, 1));
                break;

            case "DELETE":
                //TODO@torokp: Rekord törlésekor a kapcsolódó nyelvi rekordokat is törölni kellene

                $serialized = serialize($old);
                static::$db->queryDie("INSERT INTO Log SET logEventID='$nextEventID', opID='".static::$userID."', opDate=NOW(), "
                        . "logTable='$table', logEvent='DELETE', logItemID='$id', logColumn='_ALL', logValue='" . static::$db->real_escape_string($serialized) . "' ");

                static::$db->queryDie("DELETE FROM $table WHERE $key='$id' LIMIT 1");
                break;
        }

        static::$db->query("COMMIT;");
        return $id;
    }

    static public function find($table, $params, $return = "ARRAY") {
        if ($return == "ARRAY") {
            $key = "*";
        } elseif ($return == "ID") {
            $key = static::keyName($table);
        }
        
        if (is_array($params)) {
            $query = "SELECT {$key} FROM `{$table}` WHERE ";
            $sep = "";
            foreach ($params as $k => $v) {
                $query .= "{$sep} $k='$v'";
                $sep = " AND ";
            }
            $query .= " LIMIT 1";
        } else {
            $query = "SELECT {$key} FROM `{$table}` WHERE {$params} LIMIT 1";
        }
        //echo "$query";
        switch ($return) {
            case "ARRAY":
                return static::$db->queryDie($query)->fetch_assoc();

            case "ID":
                list($id) = static::$db->queryDie($query)->fetch_row();
                return $id;
        }
    }

    static public function get($table, $id) {
        $key = static::keyName($table);
        $query = "SELECT * FROM `{$table}` WHERE {$key}='{$id}' LIMIT 1";
        return static::$db->queryDie($query)->fetch_assoc();
    }

    static public function update($table, $new, $id="") {
        return static::save($table, "UPDATE", $new, $id);
    }

    static public function insert($table, $new) {
        return static::save($table, "INSERT", $new);
    }

    static public function insert_update($table, $new) {
        return static::save($table, "INSERT_UPDATE", $new);
    }

    static public function delete($table, $obj) {
        if (is_array($obj)) {
            return static::save($table, "DELETE", $obj);
        } else {
            return static::save($table, "DELETE", "", $obj);
        }
    }

}


/*
CREATE TABLE IF NOT EXISTS `Log` (
`logID` int(10) unsigned NOT NULL,
  `logEventID` int(11) NOT NULL,
  `logEvent` enum('UPDATE','DELETE','INSERT','METADATA','LEKTOR') COLLATE utf8_general_ci NOT NULL,
  `logTable` tinytext COLLATE utf8_general_ci NOT NULL,
  `logItemID` varchar(80) COLLATE utf8_general_ci NOT NULL,
  `columnType` varchar(50) COLLATE utf8_general_ci NOT NULL,
  `logColumn` tinytext COLLATE utf8_general_ci NOT NULL,
  `logValue` text COLLATE utf8_general_ci NOT NULL,
  `charCount` int(11) NOT NULL,
  `opID` int(11) NOT NULL,
  `opDate` datetime NOT NULL,
  `createID` int(11) NOT NULL,
  `createDate` timestamp NOT NULL DEFAULT '0000-00-00 00:00:00'
) ENGINE=MyISAM AUTO_INCREMENT=621312 DEFAULT CHARSET=utf8 COLLATE=utf8_general_ci;

ALTER TABLE `Log`
 ADD PRIMARY KEY (`logID`), ADD KEY `logItemID` (`logItemID`);

ALTER TABLE `Log`
MODIFY `logID` int(10) unsigned NOT NULL AUTO_INCREMENT;
*/