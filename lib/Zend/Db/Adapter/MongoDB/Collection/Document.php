<?php

class Zend_Db_Adapter_MongoDB_Collection_Document
{
    /**
     * The data for each column in the row (column_name => value).
     * The keys must match the physical names of columns in the
     * collection for which this document is defined.
     *
     * @var array
     */
    protected $_data = array();

    /**
     * This is set to a copy of $_data when the data is fetched from
     * a database, specified as a new tuple in the constructor, or
     * when dirty data is posted to the database with save().
     *
     * @var array
     */
    protected $_cleanData = array();

    /**
     * Tracks columns where data has been updated. Allows more specific insert and
     * update operations.
     *
     * @var array
     */
    protected $_modifiedFields = array();

    /**
     * Db_MongoDB_Collection_Abstract parent class or instance.
     *
     * @var Db_MongoDB_Collection_Abstract
     */
    protected $_collection = null;

    /**
     * Connected is true if we have a reference to a live
     * Db_MongoDB_Collection_Abstract object.
     * This is false after the Document has been deserialized.
     *
     * @var boolean
     */
    protected $_connected = true;

    /**
     * A row is marked read only if it contains columns that are not physically represented within
     * the database schema (e.g. evaluated columns/Zend_Db_Expr columns). This can also be passed
     * as a run-time config options as a means of protecting row data.
     *
     * @var boolean
     */
    protected $_readOnly = false;

    /**
     * Name of the class of the Db_MongoDB_Collection_Abstract object.
     *
     * @var string
     */
    protected $_collectionClass = null;

    /**
     * Primary row key(s).
     *
     * @var array
     */
    protected $_primary = "_id";

    /**
     * Constructor.
     *
     * Supported params for $config are:-
     * - collection  = class name or object of type Db_MongoDB_Collection_Abstract
     * - data        = values of columns in this row.
     *
     * @param  array $config OPTIONAL Array of user-specified config options.
     * @return void
     * @throws Db_MongoDB_Collection_Exception
     */
    public function __construct(array $config = array())
    {
        if (isset($config['collection']) && $config['collection'] instanceof Db_MongoDB_Collection_Abstract) {
            $this->_collection = $config['collection'];
            $this->_collectionClass = get_class($this->_collection);
        } elseif ($this->_collectionClass !== null) {
            $this->_collectionClass = $this->_getCollectionFromString($this->_collectionClass);
        }

        if (isset($config['data'])) {
            if (!is_array($config['data'])) {
                throw new Zend_Db_Adapter_MongoDB_Collection_Exception('Data must be an array');
            }
            $this->_data = $config['data'];
        }
        if (isset($config['stored']) && $config['stored'] === true) {
            $this->_cleanData = $this->_data;
        }

        if (isset($config['readOnly']) && $config['readOnly'] === true) {
            $this->setReadOnly(true);
        }

        $this->init();
    }

    /**
     * Transform a column name from the user-specified form
     * to the physical form used in the database.
     * You can override this method in a custom Row class
     * to implement column name mappings, for example inflection.
     *
     * @param string $columnName Column name given.
     * @return string The column name after transformation applied (none by default).
     * @throws Db_MongoDB_Collection_Exception if the $columnName is not a string.
     */
    protected function _transformColumn($columnName)
    {
        if (!is_string($columnName)) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception('Specified column is not a string');
        }
        // Perform no transformation by default
        return $columnName;
    }

    /**
     * Retrieve row field value
     *
     * @param  string $columnName The user-specified column name.
     * @return string             The corresponding column value.
     * @throws  if the $columnName is not a column in the row.
     */
    public function __get($columnName)
    {
        $columnName = $this->_transformColumn($columnName);
        if (!array_key_exists($columnName, $this->_data)) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception("Specified column \"$columnName\" is not in the row");
        }
        return $this->_data[$columnName];
    }

    /**
     * Set row field value. It doesn't need to check if the column exists or not. We are dealing
     * with documents so it is schemaless.
     *
     * @param  string $columnName The column key.
     * @param  mixed  $value The value for the property.
     * @return void
     */
    public function __set($columnName, $value)
    {
        $columnName = $this->_transformColumn($columnName);

        $this->_data[$columnName] = $value;
        $this->_modifiedFields[$columnName] = true;
    }
    /**
     * Unset row field value.
     *
     * @param  string $columnName The column key.
     * @return Db_MongoDB_Collection_Document
     * @throws Db_MongoDB_Collection_Exception
     */
    public function __unset($columnName)
    {
        $columnName = $this->_transformColumn($columnName);
        if (!array_key_exists($columnName, $this->_data)) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception("Specified column \"$columnName\" is not in the row");
        }
        if ($this->isConnected() && in_array($columnName, $this->_collection->info('primary'))) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception("Specified column \"$columnName\" is a primary key and should not be unset");
        }
        unset($this->_data[$columnName]);
        return $this;
    }

    /**
     * Test existence of row field
     *
     * @param  string  $columnName   The column key.
     * @return boolean
     */
    public function __isset($columnName)
    {
        $columnName = $this->_transformColumn($columnName);
        return array_key_exists($columnName, $this->_data);
    }

    /**
     * Store table, primary key and data in serialized object
     *
     * @return array
     */
    public function __sleep()
    {
        return array('_collectionClass', '_primary', '_data', '_cleanData', '_readOnly' ,'_modifiedFields');
    }

    /**
     * Setup to do on wakeup.
     * A de-serialized Row should not be assumed to have access to a live
     * database connection, so set _connected = false.
     *
     * @return void
     */
    public function __wakeup()
    {
        $this->_connected = false;
    }

    /**
     * Proxy to __isset
     * Required by the ArrayAccess implementation
     *
     * @param string $offset
     * @return boolean
     */
    public function offsetExists($offset)
    {
        return $this->__isset($offset);
    }

    /**
     * Proxy to __get
     * Required by the ArrayAccess implementation
     *
     * @param string $offset
     * @return string
     */
     public function offsetGet($offset)
     {
         return $this->__get($offset);
     }

     /**
      * Proxy to __set
      * Required by the ArrayAccess implementation
      *
      * @param string $offset
      * @param mixed $value
      */
     public function offsetSet($offset, $value)
     {
         $this->__set($offset, $value);
     }

     /**
      * Proxy to __unset
      * Required by the ArrayAccess implementation
      *
      * @param string $offset
      */
     public function offsetUnset($offset)
     {
         return $this->__unset($offset);
     }

    /**
     * Initialize object
     *
     * Called from {@link __construct()} as final step of object instantiation.
     *
     * @return void
     */
    public function init()
    {
    }

    /**
     * Returns the table object, or null if this is disconnected row
     *
     * @return Db_MongoDB_Collection_Abstract|null
     */
    public function getCollection()
    {
        return $this->_collection;
    }

    /**
     * Set the table object, to re-establish a live connection
     * to the database for a Row that has been de-serialized.
     *
     * @param Db_MongoDB_Collection_Abstract $collection
     * @return boolean
     * @throws Db_MongoDB_Collection_Exception
     */
    public function setCollection(Db_MongoDB_Collection_Abstract $collection = null)
    {
        if ($collection == null) {
            $this->_collection = null;
            $this->_connected = false;
            return false;
        }

        $collectionClass = get_class($collection);
        if (! $collection instanceof $this->_collectionClass) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception("The specified Table is of class $collectionClass, expecting class to be instance of $this->_collectionClass");
        }

        $this->_collection = $collection;
        $this->_collectionClass = $collectionClass;
//
//        $info = $this->_collection->info();
//
//        if ($info['cols'] != array_keys($this->_data)) {
//            require_once 'Db/Mongodb/Collection/Exception.php';
//            throw new Db_MongoDB_Collection_Exception('The specified Collectio does not have the same columns as the Row');
//        }

        if (! array_intersect((array) $this->_primary, $info['primary']) == (array) $this->_primary) {

            require_once 'Db/Mongodb/Collection/Exception.php';
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception("The specified Table '$collectionClass' does not have the same primary key as the Row");
        }

        $this->_connected = true;
        return true;
    }

    /**
     * Query the class name of the Table object for which this
     * Row was created.
     *
     * @return string
     */
    public function getCollectionClass()
    {
        return $this->_collectionClass;
    }

    /**
     * Test the connected status of the row.
     *
     * @return boolean
     */
    public function isConnected()
    {
        return $this->_connected;
    }

    /**
     * Test the read-only status of the row.
     *
     * @return boolean
     */
    public function isReadOnly()
    {
        return $this->_readOnly;
    }

    /**
     * Set the read-only status of the row.
     *
     * @param boolean $flag
     * @return boolean
     */
    public function setReadOnly($flag)
    {
        $this->_readOnly = (bool) $flag;
    }

     /**
     * Saves the properties to the database.
     *
     * This performs an intelligent insert/update, and reloads the
     * properties with fresh data from the table on success.
     *
     * @return mixed The primary key value(s), as an associative array if the
     *     key is compound, or a scalar if the key is single-column.
     */
    public function save()
    {
        /**
         * If the _cleanData array is empty,
         * this is an INSERT of a new row.
         * Otherwise it is an UPDATE.
         */
        if (empty($this->_cleanData)) {
            return $this->_doInsert();
        } else {
            return $this->_doUpdate();
        }
    }

    /**
     * @return mixed The primary key value(s), as an associative array if the
     *     key is compound, or a scalar if the key is single-column.
     */
    protected function _doInsert()
    {
        /**
         * A read-only row cannot be saved.
         */
        if ($this->_readOnly === true) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception('This row has been marked read-only');
        }

        /**
         * Run pre-INSERT logic
         */
        $this->_insert();

        /**
         * Execute the INSERT (this may throw an exception)
         */
        $data = array_intersect_key($this->_data, $this->_modifiedFields);
        $primaryKey = $this->_getCollection()->insert($data);

        /**
         * Normalize the result to an array indexed by primary key column(s).
         * The table insert() method may return a scalar.
         */
        if (is_array($primaryKey)) {
            $newPrimaryKey = $primaryKey;
        } else {
            //ZF-6167 Use tempPrimaryKey temporary to avoid that zend encoding fails.
            $tempPrimaryKey = (array) $this->_primary;
            $newPrimaryKey = array(current($tempPrimaryKey) => $primaryKey);
        }

        /**
         * Save the new primary key value in _data.  The primary key may have
         * been generated by a sequence or auto-increment mechanism, and this
         * merge should be done before the _postInsert() method is run, so the
         * new values are available for logging, etc.
         */
        $this->_data = array_merge($this->_data, $newPrimaryKey);

        /**
         * Run post-INSERT logic
         */
        $this->_postInsert();

        /**
         * Update the _cleanData to reflect that the data has been inserted.
         */
        $this->_refresh();

        return $primaryKey;
    }

    /**
     * @return mixed The primary key value(s), as an associative array if the
     *     key is compound, or a scalar if the key is single-column.
     */
    protected function _doUpdate()
    {
        /**
         * A read-only row cannot be saved.
         */
        if ($this->_readOnly === true) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception('This row has been marked read-only');
        }

        /**
         * Get expressions for a WHERE clause
         * based on the primary key value(s).
         */
        $where = $this->_getWhereQuery(false);

        /**
         * Run pre-UPDATE logic
         */
        $this->_update();

        /**
         * Compare the data to the modified fields array to discover
         * which columns have been changed.
         */
        $diffData = array_intersect_key($this->_data, $this->_modifiedFields);

        /**
         * Were any of the changed columns part of the primary key?
         */
        $pkDiffData = array_intersect_key($diffData, array_flip((array)$this->_primary));

        /**
         * Execute cascading updates against dependent tables.
         * Do this only if primary key value(s) were changed.
         */
//        if (count($pkDiffData) > 0) {
//            $depTables = $this->_getCollection()->getDependentTables();
//            if (!empty($depTables)) {
//                $pkNew = $this->_getPrimaryKey(true);
//                $pkOld = $this->_getPrimaryKey(false);
//                foreach ($depTables as $collectionClass) {
//                    $t = $this->_getCollectionFromString($collectionClass);
//                    $t->_cascadeUpdate($this->getCollectionClass(), $pkOld, $pkNew);
//                }
//            }
//        }

        /**
         * Execute the UPDATE (this may throw an exception)
         * Do this only if data values were changed.
         * Use the $diffData variable, so the UPDATE statement
         * includes SET terms only for data values that changed.
         */
        if (count($diffData) > 0) {
            $this->_getCollection()->update($diffData, $where);
        }

        /**
         * Run post-UPDATE logic.  Do this before the _refresh()
         * so the _postUpdate() function can tell the difference
         * between changed data and clean (pre-changed) data.
         */
        $this->_postUpdate();

        /**
         * Refresh the data just in case triggers in the RDBMS changed
         * any columns.  Also this resets the _cleanData.
         */
        $this->_refresh();

        /**
         * Return the primary key value(s) as an array
         * if the key is compound or a scalar if the key
         * is a scalar.
         */
        $primaryKey = $this->_getPrimaryKey(true);
        if (count($primaryKey) == 1) {
            return current($primaryKey);
        }

        return $primaryKey;
    }

    /**
     * Deletes existing rows.
     *
     * @return int The number of rows deleted.
     */
    public function delete()
    {
        /**
         * A read-only row cannot be deleted.
         */
        if ($this->_readOnly === true) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception('This row has been marked read-only');
        }

        $where = $this->_getWhereQuery();

        /**
         * Execute pre-DELETE logic
         */
        $this->_delete();

        /**
         * Execute cascading deletes against dependent tables
         */
//        $depTables = $this->_getCollection()->getDependentTables();
//        if (!empty($depTables)) {
//            $pk = $this->_getPrimaryKey();
//            foreach ($depTables as $collectionClass) {
//                $t = $this->_getCollectionFromString($collectionClass);
//                $t->_cascadeDelete($this->getCollectionClass(), $pk);
//            }
//        }

        /**
         * Execute the DELETE (this may throw an exception)
         */
        $result = $this->_getCollection()->delete($where);

        /**
         * Execute post-DELETE logic
         */
        $this->_postDelete();

        /**
         * Reset all fields to null to indicate that the row is not there
         */
        $this->_data = array_combine(
            array_keys($this->_data),
            array_fill(0, count($this->_data), null)
        );

        return $result;
    }

    /**
     * Returns the column/value data as an array.
     *
     * @return array
     */
    public function toArray()
    {
        return (array)$this->_data;
    }

    /**
     * Sets all data in the row from an array.
     *
     * @param  array $data
     * @return Zend_Db_collection_Row_Abstract Provides a fluent interface
     */
    public function setFromArray(array $data)
    {
        $data = array_intersect_key($data, $this->_data);

        foreach ($data as $columnName => $value) {
            $this->__set($columnName, $value);
        }

        return $this;
    }

    /**
     * Refreshes properties from the database.
     *
     * @return void
     */
    public function refresh()
    {
        return $this->_refresh();
    }

    /**
     * Retrieves an instance of the parent table.
     *
     * @return Db_MongoDB_Collection_Abstract
     */
    protected function _getCollection()
    {
        if (!$this->_connected) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception('Cannot save a Row unless it is connected');
        }
        return $this->_collection;
    }

    /**
     * Retrieves an associative array of primary keys.
     *
     * @param bool $useDirty
     * @return array
     */
    protected function _getPrimaryKey($useDirty = true)
    {
        if (!is_array($this->_primary)) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception("The primary key must be set as an array");
        }

        $primary = array_flip($this->_primary);
        if ($useDirty) {
            $array = array_intersect_key($this->_data, $primary);
        } else {
            $array = array_intersect_key($this->_cleanData, $primary);
        }
        if (count($primary) != count($array)) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception("The specified Table '$this->_collectionClass' does not have the same primary key as the Row");
        }
        return $array;
    }

    /**
     * Constructs where statement for retrieving row(s).
     *
     * @param bool $useDirty
     * @return array
     */
    protected function _getWhereQuery($useDirty = true)
    {
        $where = array();
        $where = $this->_getPrimaryKey($useDirty);
        return $where;
    }

    /**
     * Refreshes properties from the database.
     *
     * @return void
     */
    protected function _refresh()
    {
        $where = $this->_getWhereQuery();
        $row = $this->_getCollection()->fetchRow($where);

        if (null === $row) {
            throw new Zend_Db_Adapter_MongoDB_Collection_Exception('Cannot refresh row as parent is missing');
        }

        $this->_data = $row->toArray();
        $this->_cleanData = $this->_data;
        $this->_modifiedFields = array();
    }

    /**
     * Allows pre-insert logic to be applied to row.
     * Subclasses may override this method.
     *
     * @return void
     */
    protected function _insert()
    {
    }

    /**
     * Allows post-insert logic to be applied to row.
     * Subclasses may override this method.
     *
     * @return void
     */
    protected function _postInsert()
    {
    }

    /**
     * Allows pre-update logic to be applied to row.
     * Subclasses may override this method.
     *
     * @return void
     */
    protected function _update()
    {
    }

    /**
     * Allows post-update logic to be applied to row.
     * Subclasses may override this method.
     *
     * @return void
     */
    protected function _postUpdate()
    {
    }

    /**
     * Allows pre-delete logic to be applied to row.
     * Subclasses may override this method.
     *
     * @return void
     */
    protected function _delete()
    {
    }

    /**
     * Allows post-delete logic to be applied to row.
     * Subclasses may override this method.
     *
     * @return void
     */
    protected function _postDelete()
    {
    }

    /**
     * Turn magic function calls into non-magic function calls
     * to the above methods.
     *
     * @param string $method
     * @param array $args
     * @return Db_MongoDB_Collection_Document_Abstract|Db_MongoDB_Collection_Documentset_Abstract
     * @throws Db_MongoDB_Collection_Exception If an invalid method is called.
     */
    public function __call($method, array $args)
    {
//        $matches = array();

//        /**
//         * Recognize methods for Has-Many cases:
//         * findParent<Class>()
//         * findParent<Class>By<Rule>()
//         * Use the non-greedy pattern repeat modifier e.g. \w+?
//         */
//        if (preg_match('/^findParent(\w+?)(?:By(\w+))?$/', $method, $matches)) {
//            $class    = $matches[1];
//            $ruleKey1 = isset($matches[2]) ? $matches[2] : null;
//            return $this->findParentRow($class, $ruleKey1, $select);
//        }
//
//        /**
//         * Recognize methods for Many-to-Many cases:
//         * find<Class1>Via<Class2>()
//         * find<Class1>Via<Class2>By<Rule>()
//         * find<Class1>Via<Class2>By<Rule1>And<Rule2>()
//         * Use the non-greedy pattern repeat modifier e.g. \w+?
//         */
//        if (preg_match('/^find(\w+?)Via(\w+?)(?:By(\w+?)(?:And(\w+))?)?$/', $method, $matches)) {
//            $class    = $matches[1];
//            $viaClass = $matches[2];
//            $ruleKey1 = isset($matches[3]) ? $matches[3] : null;
//            $ruleKey2 = isset($matches[4]) ? $matches[4] : null;
//            return $this->findManyToManyRowset($class, $viaClass, $ruleKey1, $ruleKey2, $select);
//        }
//
//        /**
//         * Recognize methods for Belongs-To cases:
//         * find<Class>()
//         * find<Class>By<Rule>()
//         * Use the non-greedy pattern repeat modifier e.g. \w+?
//         */
//        if (preg_match('/^find(\w+?)(?:By(\w+))?$/', $method, $matches)) {
//            $class    = $matches[1];
//            $ruleKey1 = isset($matches[2]) ? $matches[2] : null;
//            return $this->findDependentRowset($class, $ruleKey1, $select);
//        }

        throw new Zend_Db_Adapter_MongoDB_Collection_Exception("Unrecognized method '$method()'");
    }


    /**
     * _getCollectionFromString
     *
     * @param string $collectionName
     * @return Db_MongoDB_Collection_Abstract
     */
    protected function _getCollectionFromString($collectionName)
    {
        if ($this->_collection instanceof Db_MongoDB_Collection_Abstract) {
            return new Db_MongoDB_Collection($collectionName);
        }

        // assume the tableName is the class name
        if (!class_exists($collectionName)) {
            try {
                Zend_Loader::loadClass($collectionName);
            } catch (Zend_Exception $e) {
                throw new Zend_Db_Adapter_MongoDB_Collection_Exception($e->getMessage(), $e->getCode(), $e);
            }
        }

        $options = array();

        if (($collection = $this->_getCollection())) {
            $options['db'] = $collection->getAdapter();
        }

        if (isset($collectionDefinition) && $collectionDefinition !== null) {
            $options[Db_MongoDB_Collection_Abstract::DEFINITION] = $collectionDefinition;
        }

        return new $collectionName($options);
    }

}
