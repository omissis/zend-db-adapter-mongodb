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
class Zend_Db_Adapter_MongoDB extends Zend_Db_Adapter_Abstract
{
    /**
     * @var array
     */
    protected $_connOptions = array(
        "connect" => FALSE,
        "timeout" => 5000
    );

    /**
     * @var \Mongo
     */
    protected $_connection;

    /**
     * @var array
     */
    protected $_config;

    /**
     * @var \MongoDB
     */
    protected $_db;

    public function __construct(array $config)
    {
        $this->_checkRequiredOptions($config);

        $this->_config = $config;

        $host = 'mongodb://' . $config['host'] . ':' . $config['port'];

        if (!empty($config["username"])) {
            $this->_connOptions["username"] = $config["username"];
        }

        if (!empty($config["password"])) {
            $this->_connOptions["password"] = $config["password"];
        }

        $this->_connection = new Mongo($host . '/' . $config["dbname"], $this->_connOptions);

        return $this->_connection;
    }

    public function getDbName()
    {
        return $this->_config['dbname'];
    }

    public function getPassword()
    {
        return $this->_config['password'];
    }

    public function getUsername()
    {
        return $this->_config['username'];
    }

    public function getHost()
    {
        return $this->_config['host'];
    }

    public function getPort()
    {
        return $this->_config['port'];
    }

    public function getConnection()
    {
        if ($this->_connOptions["connect"] == FALSE) {
            $this->_connection->connect();
        }

        return $this->_connection;
    }

    public function setUpDatabase($db = null)
    {
        $conn = $this->getConnection();

        if ($db !== null) {
            $this->_config['dbname'] = $db;
        }

        $this->_db = $conn->selectDB($this->getDbName());

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
            throw new Zend_Db_Adapter_MongoDB_Exception("MongoDB Connection not initialized");
        }

        if (method_exists($this->_db, $fn)) {
            return call_user_func_array(array($this->_db, $fn), $args);
        }

        throw new Zend_Db_Adapter_MongoDB_Exception("MongoDB::{$fn} Method not found");
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
            throw new Zend_Db_Adapter_MongoDB_Exception("Configuration array must have a key for 'host'");
        }
        if (!array_key_exists('port', $config)) {
            throw new Zend_Db_Adapter_MongoDB_Exception("Configuration array must have a key for 'port'");
        }
    }
}