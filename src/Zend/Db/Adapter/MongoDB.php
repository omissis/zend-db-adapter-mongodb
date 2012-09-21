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

    /**
     *
     * @param array $config
     *
     * @return \Mongo
     */
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

        $this->setUpDatabase();

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

    public function query($query, $bind = array())
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

    protected function _checkRequiredOptions(array $config)
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

    /** Abstract methods implementations **/

    /**
     * Returns a list of the collections in the database.
     *
     * @return array
     */
    public function listTables() {
        return $this->_db->listCollections();
    }

    /**
     * @see self::listTables()
     */
    public function listCollections() {
        return $this->listTables();
    }

    /**
     * @todo improve
     */
    public function describeTable($tableName, $schemaName = null) {
        return array();
    }

    /**
     * {@inheritdoc}
     */
    protected function _connect() {
        if ($this->_connOptions["connect"] == FALSE) {
            $this->_connection->connect();
        }
    }

    /**
     * {@inheritdoc}
     */
    public function isConnected() {
        return $this->_connection->connected;
    }

    /**
     * {@inheritdoc}
     */
    public function closeConnection() {
        return $this->_connection->close();
    }

    public function prepare($sql) {
        throw new Exception("Cannot prepare statements in MongoDB");
    }

    public function lastInsertId($tableName = null, $primaryKey = null) {
        return null;
    }

    protected function _beginTransaction() {
        throw new Exception("There are no transactions in MongoDB");
    }

    protected function _commit() {
        throw new Exception("There are no commits(ie: transactions) in MongoDB");
    }

    protected function _rollBack() {
        throw new Exception("There are no rollbacks(ie: transactions) in MongoDB");
    }

    /**
     * @todo improve
     */
    public function setFetchMode($mode) {

    }

    /**
     * @todo improve
     */
    public function limit($sql, $count, $offset = 0) {

    }

    /**
     * @todo improve
     */
    public function supportsParameters($type) {
        return false;
    }

    /**
     * {@inheritdoc}
     */
    public function getServerVersion() {
        return \Mongo::VERSION;
    }
}