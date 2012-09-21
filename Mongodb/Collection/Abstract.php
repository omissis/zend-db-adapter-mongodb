<?php
require_once 'Db/Mongodb/Adapter.php';

abstract class Db_Mongodb_Collection_Abstract
{
    const ADAPTER                  = 'db';
    const DATABASE                    = 'dbname';
    const NAME                      = 'name';
    const PRIMARY                   = 'primary';
    const DOCUMENT_CLASS            = 'documentClass';
    const DOCUMENTSET_CLASS         = 'documentsetClass';
    const METADATA                  = 'metadata';
    const COLS                       = 'columns';

    const DEFAULT_NONE              = 'defaultNone';
    const DEFAULT_CLASS             = 'defaultClass';
    const DEFAULT_DB                = 'defaultDb';

    /**
     * Default Db_Mongodb_Adapter object.
     *
     * @var Db_Mongodb_Adapter
     */
    protected static $_defaultDb;

     /**
     * Db_Mongodb_Adapter object.
     *
     * @var Db_Mongodb_Adapter
     */
    protected $_db;

    /**
     * The database / schema name (default null means current schema)
     *
     * @var array
     */
    protected $_database = null;

    /**
     * The collection name.
     *
     * @var string
     */
    protected $_name = null;

    /**
     * The collection field / column names derived as set by SetOptions. Columns to fetch / show.
     *
     * @var array
     */
    protected $_cols;

    /**
     * The primary key or Object id of the document.
     * A compound key should be declared as an array.
     * You may declare a single-column primary key
     * as a string.
     *
     * @var mixed
     */
    protected $_primary = "_id";

    /**
     * Some specified collection field information we'd like to work against.
     *
     * @var array
     */
    protected $_metadata = array();

    /**
     * If your primary key is a compound key, and one of the columns uses
     * an auto-increment or sequence-generated value, set _identity
     * to the ordinal index in the $_primary array for that column.
     * Note this index is the position of the column in the primary key,
     * not the position of the column in the collection.  The primary key
     * array is 1-based.
     *
     * @var integer
     */
    protected $_identity = 1;

    /**
     * Define the logic for new values in the primary key.
     * May be a string, boolean true, or boolean false.
     *
     * @var mixed
     */
    protected $_sequence;

    /**
     * Classname for Document
     *
     * @var string
     */
    protected $_documentClass = 'Db_Mongodb_Collection_Document';

    /**
     * Classname for Document set
     *
     * @var string
     */
    protected $_documentsetClass = 'Db_Mongodb_Collection_Documentset';
    protected $_defaultSource = self::DEFAULT_NONE;
    protected $_defaultValues = array();

    /**
     * Constructor.
     *
     * Supported params for $config are:
     * - db               = user-supplied instance of database connector,
     *                     or key name of registry instance.
     * - name             = collection name.
     * - primary          = string or array of primary key(s).
     * - documentClass    = row class name.
     * - documentsetClass = rowset class name.
     *
     * @param  mixed $config Array of user-specified config options, or just the Db Adapter.
     * @return void
     */
    public function __construct($config = array())
    {
        /**
         * Allow a scalar argument to be the Adapter object or Registry key. The adapter fed takes precedence
         * over creating a new adapter from provided configuration values that's why they are in the same
         * logical structure
         */
        if (isset($config) && !is_array($config)) {

            $config = array(self::ADAPTER => $config);

        } elseif (!empty($config["dbname"]) && !empty($config["host"])) {

            $par["dbname"]   = $config["dbname"];
            $par["username"] = !empty($config["username"]) ? $config["username"] : "";
            $par["password"] = !empty($config["password"]) ? $config["password"] : "";
            $par["host"]     = $config["host"];

            require_once("Db/Mongodb/Adapter.php");
            $this->_db = new Db_Mongodb_Adapter($par);
        }

        //always course through setting up the options
        if ($config) {
            $this->setOptions($config);
        }

        $this->_setup();
        $this->init();
    }

    /**
     * setOptions()
     *
     * @param array $options
     * @return Db_Mongodb_Collection_Abstract
     */
    public function setOptions(Array $options)
    {
        foreach ($options as $key => $value) {
            switch ($key) {
                case self::ADAPTER:
                    $this->_setAdapter($value);
                    break;
                case self::DATABASE :
                    $this->_database = (string) $value;
                    break;
                case self::NAME:
                    $this->_name = (string) $value;
                    break;
                case self::PRIMARY:
                    $this->_primary = (array) $value;
                    break;
                case self::DOCUMENT_CLASS:
                    $this->setDocumentClass($value);
                    break;
                case self::DOCUMENTSET_CLASS:
                    $this->setDocumentsetClass($value);
                    break;
                default:
                    // ignore unrecognized configuration directive
                    break;
            }
        }
        return $this;
    }

    /**
     * @param  string $classname
     * @return Db_Mongodb_Collection_Abstract Provides a fluent interface
     */
    public function setDocumentClass($classname)
    {
        $this->_documentClass = (string) $classname;

        return $this;
    }

    /**
     * @return string
     */
    public function getDocumentClass()
    {
        return $this->_documentClass;
    }

    /**
     * @return string
     */
    public function getCollectionName()
    {
        return $this->_name;
    }

    /**
     * @param  string $classname
     * @return Zend_Db_collection_Abstract Provides a fluent interface
     */
    public function setDocumentsetClass($classname)
    {
        $this->_documentsetClass = (string) $classname;

        return $this;
    }

    /**
     * @return string
     */
    public function getDocumentsetClass()
    {
        return $this->_documentsetClass;
    }
    /**
     * set the defaultSource property - this tells the collection class where to find default values
     *
     * @param string $defaultSource
     * @return Db_Mongodb_Collection_Abstract
     */
    public function setDefaultSource($defaultSource = self::DEFAULT_NONE)
    {
        if (!in_array($defaultSource, array(self::DEFAULT_CLASS, self::DEFAULT_DB, self::DEFAULT_NONE))) {
            $defaultSource = self::DEFAULT_NONE;
        }

        $this->_defaultSource = $defaultSource;
        return $this;
    }

    /**
     * returns the default source flag that determines where defaultSources come from
     *
     * @return unknown
     */
    public function getDefaultSource()
    {
        return $this->_defaultSource;
    }

    /**
     * set the default values for the collection class
     *
     * @param array $defaultValues
     * @return Zend_Db_collection_Abstract
     */
    public function setDefaultValues(Array $defaultValues)
    {
        foreach ($defaultValues as $defaultName => $defaultValue) {
            if (array_key_exists($defaultName, $this->_metadata)) {
                $this->_defaultValues[$defaultName] = $defaultValue;
            }
        }
        return $this;
    }

    public function getDefaultValues()
    {
        return $this->_defaultValues;
    }


    /**
     * Sets the default Db_Mongodb_Adapter for all Db_Mongodb_Collection objects.
     *
     * @param  mixed $db Either an Adapter object, or a string naming a Registry key
     * @return void
     */
    public static function setDefaultAdapter($db = null)
    {
        self::$_defaultDb = self::_setupAdapter($db);
    }

    /**
     * Gets the default Db_Mongodb_Adapter for all Db_Mongodb_Collection objects.
     *
     * @return Db_Mongodb_Adapter or null
     */
    public static function getDefaultAdapter()
    {
        return self::$_defaultDb;
    }

    /**
     * @param  mixed $db Either an Adapter object, or a string naming a Registry key
     * @return Db_Mongodb_Adapter Provides a fluent interface
     */
    protected function _setAdapter($db)
    {
        $this->_db = self::_setupAdapter($db);
        return $this;
    }

    /**
     * Gets the Db_Mongodb_Adapter for this particular Zend_Db_collection object.
     *
     * @return Db_Mongodb_Adapter
     */
    public function getAdapter()
    {
        return $this->_db;
    }

    /**
     * @param  mixed $db Either an Adapter object, or a string naming a Registry key
     * @return Db_Mongodb_Adapter
     * @throws Db_Mongodb_Collection_Exception
     */
    protected static function _setupAdapter($db)
    {
        if ($db === null) {
            return null;
        }
        if (is_string($db)) {
            require_once 'Zend/Registry.php';
            $db = Zend_Registry::get($db);
        }
        if (!$db instanceof Db_Mongodb_Adapter) {
            require_once 'Db/Mongodb/Collection/Exception.php';
            throw new Db_Mongodb_Collection_Exception('Argument must be of type Db_Mongodb_Adapter, or a Registry key where a Db_Mongodb_Adapter object is stored');
        }
        return $db;
    }

    /**
     * Sets the sequence member, which defines the behavior for generating
     * primary key values in new rows.
     * - If this is a string, then the string names the sequence object.
     * - If this is boolean true, then the key uses an auto-incrementing
     *   or identity mechanism.
     * - If this is boolean false, then the key is user-defined.
     *   Use this for natural keys, for example.
     *
     * @param mixed $sequence
     * @return Db_Mongodb_Adapter Provides a fluent interface
     */
    protected function _setSequence($sequence)
    {
        $this->_sequence = $sequence;

        return $this;
    }

    /**
     * Turnkey for initialization of a collection object.
     * Calls other protected methods for individual tasks, to make it easier
     * for a subclass to override part of the setup logic.
     *
     * @return void
     */
    protected function _setup()
    {
        $this->_setupDatabaseAdapter();
        $this->_setupCollectionName();
        if ($this->_database) {
            $this->_db->setUpDatabase($this->_database);
        }
    }

    /**
     * Initialize database adapter.
     *
     * @return void
     */
    protected function _setupDatabaseAdapter()
    {
        if (! $this->_db) {
            $this->_db = self::getDefaultAdapter();
            if (!$this->_db instanceof Db_Mongodb_Adapter) {
                require_once 'Db/Mongodb/Collection/Exception.php';
                throw new Db_Mongodb_Collection_Exception('No adapter found for ' . get_class($this));
            }
        }
    }

    /**
     * Initialize collection and database names.
     *
     * If the collection name is not set in the class definition,
     * use the class name itself as the collection name.
     *
     * A database name provided with the collection name (e.g., "db.collection") overrides
     * any existing value for $this->_database.
     *
     * @return void
     */
    protected function _setupCollectionName()
    {
        if (! $this->_name) {
            $this->_name = get_class($this);
        } else if (strpos($this->_name, '.')) {
            list($this->_database, $this->_name) = explode('.', $this->_name);
        }
    }

    /**
     * Initializes metadata.
     *
     * @todo find a way to dynamically load metadata from a source.
     *
     * @return boolean
     */
    protected function _setupMetadata($metadata)
    {
        if ((count($this->_metadata) > 0)) {
            return true;
        }
        // Assign the metadata to $this
        $this->_metadata = $metadata;
        // Return whether the metadata were loaded from cache
        return true;
    }

    /**
     * Retrieve collection columns
     *
     * @return array
     */
    protected function _getCols()
    {
        if (null === $this->_cols) {
            $this->_setupMetadata();
            $this->_cols = array_keys($this->_metadata);
        }
        return $this->_cols;
    }

    /**
     * Initialize primary key from metadata.
     *
     * @return void
     * @throws Db_Mongodb_Collection_Exception
     */
    protected function _setupPrimaryKey()
    {
        if (!$this->_primary) {
            $this->_setupMetadata();
            $this->_primary = array();
            foreach ($this->_metadata as $col) {
                if ($col['OBJECT_ID']) {
                    $this->_primary[0] = $col['OBJECT_ID'];
                    $this->_identity = 1;
                }
            }
            // if no primary key was specified and none was found in the metadata
            // then throw an exception.
            if (empty($this->_primary)) {
                require_once 'Db/Mongodb/Collection/Exception.php';
                throw new Db_Mongodb_Collection_Exception('A collection must have a its Object ID or pseudo-primary key defined, but none was found');
            }
        } else if (!is_array($this->_primary)) {
            $this->_primary = array(1 => $this->_primary);
        } else if (isset($this->_primary[0])) {
            array_unshift($this->_primary, null);
            unset($this->_primary[0]);
        }

        $primary    = (array) $this->_primary;
        $pkIdentity = $primary[(int) $this->_identity];
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
     * Returns collection information.
     *
     * You can elect to return only a part of this information by supplying its key name,
     * otherwise all information is returned as an array.
     *
     * @param  $key The specific info part to return OPTIONAL
     * @return mixed
     */
    public function info($key = null)
    {
        $this->_setupPrimaryKey();

        $info = array(
            self::DATABASE              => $this->_database,
            self::NAME                    => $this->_name,
            self::COLS                    => $this->_getCols(),
            self::PRIMARY                  => (array) $this->_primary,
            self::METADATA                 => $this->_metadata,
            self::DOCUMENT_CLASS           => $this->getDocumentClass(),
            self::DOCUMENTSET_CLASS     => $this->getDocumentsetClass(),
        );

        if ($key === null) {
            return $info;
        }

        if (!array_key_exists($key, $info)) {
            require_once 'Db/Mongodb/Collection/Exception.php';
            throw new Db_Mongodb_Collection_Exception('There is no collection information for the key "' . $key . '"');
        }

        return $info[$key];
    }

    /**
     * Inserts a new row.
     *
     * @param  array  $data  Column-value pairs.
     * @return mixed         The primary key of the row inserted.
     */
    public function insert(array $data)
    {
        $this->_setupPrimaryKey();

        $primary = (array) $this->_primary;
        $pkIdentity = $primary[(int)$this->_identity];
        /**
         * The object id is generated automatically, so just omit it from the tuple.
         */
        if (array_key_exists($pkIdentity, $data) && $data[$pkIdentity] === null) {
            unset($data[$pkIdentity]);
        }
        /**
         * INSERT the new row.
         */
        //there will always be a $this->_name because of $this->_setup
        $result = $this->_db->selectCollection($this->_name)->insert($data);
        if ($result) {
            return $result;
        }

        return FALSE;
    }

    /**
     * Updates existing rows.
     *
     * @param array $data         Column-value pairs.
     * @param array $criteria   Column-value pairs to match against existing.
     * @param array $options    i.e array("safe"=> true, "upsert"=> true, "multiple"=> true)
     * @return int
     */
    public function update(array $data, array $criteria, $options = null)
    {
        $this->_setupPrimaryKey();

        $primary = (array) $this->_primary;
        $pkIdentity = $primary[(int)$this->_identity];
        /**
         * The object id is generated automatically, so just omit it from the tuple.
         */
        if (array_key_exists($pkIdentity, $data) && $data[$pkIdentity] === null) {
            unset($data[$pkIdentity]);
        }
        //there will always be a $this->_name because of $this->_setup
        return $this->_db->selectCollection($this->_name)->update($criteria, $data, $options);
    }

    /**
     * Deletes existing rows.
     *
     * @param array $criteria Column-value pairs to match against existing.
     * @param array $options i.e array("safe"=> true, "justOne"=> true)
     * @return int
     */
    public function delete(array $criteria, $options = null)
    {
        return $this->_db->selectCollection($this->_name)->remove($criteria, $options);
    }
    public function find($id)
    {
        return $this->fetchAll("{'_id':ObjectId('$id')}", $this->_cols);
    }
    public function fetchAll($query = array(), $fields = array(), $limit = null, $order = null)
    {
        if (!empty($fields)) {
            $this->_cols = $fields;
        }
        $cursor = $this->_db->selectCollection($this->_name)->find($query, $this->_cols);
        if (!empty($limit) && $limit > 0) {
            $cursor->limit($limit);
        }
        if (!empty($order)) {
            $cursor->sort($order);
        }
//        echo $cursor->count();
        $documents = array();
        while(FALSE !== $cursor->hasNext()) {
            $documents[] = $cursor->getNext();
        }
//        print_r($documents);exit;
        $data  = array(
            'collection'    => $this,
            'data'             => $documents,
            'documentClass' => $this->getDocumentClass(),
            'stored'           => true,
            'count'            => $cursor->count()
        );

        $documentsetClass = $this->getDocumentsetClass();
        if (!class_exists($documentsetClass)) {
            require_once 'Zend/Loader.php';
            Zend_Loader::loadClass($documentsetClass);
        }
        return new $documentsetClass($data);
    }

    /**
     * Fetches one row in an object of type Db_Mongodb_Collection_Row
     * or returns null if no row matches the specified criteria.
     *
     */
    public function fetchRow($query = null, $fields = null)
    {

        if (!empty($fields)) {
            $this->_cols = $fields;
        }
        $document = $this->_db->selectCollection($this->_name)->findOne($query, $this->_cols);

        if (count($document) == 0) {
            return null;
        }
        $data = array(
            'collection'    => $this,
            'data'             => $document,
            'stored'          => true
        );

        $documentClass = $this->getDocumentClass();
        if (!class_exists($documentClass)) {
            require_once 'Zend/Loader.php';
            Zend_Loader::loadClass($documentClass);
        }
        return new $documentClass($data);
    }
}
