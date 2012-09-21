<?php
/**
 * This is an Adapter class to wrap the PHP 5 built-in classes Mongo (Connection Object)
 * and MongoDB (Database Operations Object). The concept and interfaces are very much
 * similar to a Zend Framework Db Object because it is intended to work with it seamlessly.
 * However, this can be used generically.
 *
 * You will need to read the API documentation for MongoDB and Mongo PHP objects because most of
 * the operations are used as is. *
 */
class Zend_Db_Adapter_MongoDB
{
    protected $_connOptions = array("connect" => FALSE,
                                    "timeout" => 5000);
    protected $_connection;
    protected $_db;         //instance of MongoDB
    protected $_dbname;     //name of the database

    public function __construct($config)
    {
        /*
         * Verify that adapter parameters are in an array.
         */
        if (!is_array($config)) {
            /*
             * Convert Zend_Config argument to a plain array.
             */
            if ($config instanceof Zend_Config) {
                $config = $config->toArray();
            } else {
                throw new Zend_Db_Adapter_MongoDB_Exception('Adapter parameters must be in an array or a Zend_Config object');
            }
        }
        $this->_checkRequiredOptions($config);

        $login = !empty($config["username"]) && !empty($config["password"]) ? "{$config["username"]}:{$config["password"]}" : "";
        $host = $this->_buildHostString($config["host"]);
        $db = $config["dbname"];
        try {
            if (!empty($config["persists"])) {
                //mongodb uses this value as a connection ID. If you want to create a new persistent connection
                //for each instance, make a code to generate these strings.
                $this->_connOptions["persists"] = $config["persists"];
            }
            $mongo = new Mongo("$login$host/$db", $this->_connOptions);
            $this->_connection = $mongo;
            $this->_dbname = $db;
        }
        catch (Exception $e) {
            throw new Zend_Db_Adapter_MongoDB_Exception("Adapter cannot make a mongodb connection. {$e->getMessage()}");
        }
        return $this->_connection;
    }

    public function getConnection()
    {
//        if ($this->_connOptions["connect"] == FALSE && !is_object($this->_connection)) {
        if ($this->_connOptions["connect"] == FALSE) {
            $this->_connection->connect();
        }
        return $this->_connection;
    }

    public function setUpDatabase($db = null)
    {
        $conn = $this->getConnection();
        if ($db !== null) {
            $this->_dbname = $db;
        }
//        $this->_db = $conn->selectDB($this->_dbname);
        $this->_db = $conn->{$this->_dbname};
        return $this->_db;
    }

    public function getAdapter()
    {
        return $this->_db;
    }

    public function query($query)
    {
        return $this->_db->execute($query);
    }

    public function __call($fn, $args)
    {
        if (empty($this->_db)) {
            $this->setUpDatabase($this->_dbname);
        }
        if (method_exists($this->_db, $fn)) {
            $result = call_user_func_array(array($this->_db, $fn), $args);
        } else {
            throw new Exception("MongoDB::{$fn} Method not found");
        }
        return $result;
    }

    protected function _buildHostString($hosts)
    {
        $base = "mongodb://";
        if (count($hosts) > 0 && !array_key_exists('hostname', $hosts)) {
            $mbase = "";
            foreach ($hosts as $host) {
                if ($base != "mongodb://") {
                    $mbase .= ",";
                }
                $mbase .= "{$host["hostname"]}:{$host["port"]}";
                $base .= $mbase;
                $mbase = "";
            }
        } else {
            $base .= "{$hosts["hostname"]}:{$hosts["port"]}";
        }
        return $base;
    }

    protected function _checkRequiredOptions($config)
    {
        if (!array_key_exists('dbname', $config)) {
            throw new Zend_Db_Adapter_MongoDB_Exception("Configuration array must have a key for 'dbname' that names the database instance");
        }
        if (!array_key_exists('password', $config)) {
            throw new Zend_Db_Adapter_MongoDB_Exception("Configuration array must have a key for 'password' for login credentials");
        }
        if (!array_key_exists('username', $config)) {
            throw new Zend_Db_Adapter_MongoDB_Exception("Configuration array must have a key for 'username' for login credentials");
        }
        if (!array_key_exists('host', $config)) {
            throw new Zend_Db_Adapter_MongoDB_Exception("Configuration array must have a key for 'host' for login credentials");
        }
    }
}