<?php
class Db_Mongodb_Collection_Documentset implements SeekableIterator, Countable, ArrayAccess
{
    /**
     * The original data for each row.
     *
     * @var array
     */
    protected $_data = array();

    /**
     * Db_Mongodb_Collection_Abstract object.
     *
     * @var Db_Mongodb_Collection_Abstract
     */
    protected $_collection;

    /**
     * Connected is true if we have a reference to a live
     * Db_Mongodb_Collection_Abstract object.
     * This is false after the Rowset has been deserialized.
     *
     * @var boolean
     */
    protected $_connected = true;

    /**
     * Db_Mongodb_Collection_Abstract class name.
     *
     * @var string
     */
    protected $_collectionClass;

    /**
     * Db_Mongodb_Collection_Document class name.
     *
     * @var string
     */
    protected $_documentClass = 'Db_Mongodb_Collection_Document';

    /**
     * Iterator pointer.
     *
     * @var integer
     */
    protected $_pointer = 0;

    /**
     * How many data rows there are.
     *
     * @var integer
     */
    protected $_count;

    /**
     * Collection of instantiated Db_Mongodb_Collection_Document objects.
     *
     * @var array
     */
    protected $_documents = array();

    /**
     * @var boolean
     */
    protected $_stored = false;

    /**
     * @var boolean
     */
    protected $_readOnly = false;

    /**
     * Constructor.
     *
     * @param array $config
     */
    public function __construct(array $config)
    {
        if (isset($config['collection'])) {
            $this->_collection      = $config['collection'];
            $this->_collectionClass = get_class($this->_collection);
        }
        if (isset($config['documentClass'])) {
            $this->_documentClass   = $config['documentClass'];
        }
        if (!class_exists($this->_documentClass)) {
            require_once 'Zend/Loader.php';
            Zend_Loader::loadClass($this->_documentClass);
        }
        if (isset($config['data'])) {
            $this->_data       = $config['data'];
        }
        if (isset($config['readOnly'])) {
            $this->_readOnly   = $config['readOnly'];
        }
        if (isset($config['stored'])) {
            $this->_stored     = $config['stored'];
        }

        // set the count of rows
         if (isset($config['count'])) {
            $this->_count = $config['count'];
        } else {
            $this->_count = count($this->_data);
        }

        $this->init();
    }

    /**
     * Store data, class names, and state in serialized object
     *
     * @return array
     */
    public function __sleep()
    {
        return array('_data', '_collectionClass', '_documentClass', '_pointer', '_count', '_documents', '_stored',
                     '_readOnly');
    }

    /**
     * Setup to do on wakeup.
     * A de-serialized Rowset should not be assumed to have access to a live
     * database connection, so set _connected = false.
     *
     * @return void
     */
    public function __wakeup()
    {
        $this->_connected = false;
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
     * Return the connected state of the rowset.
     *
     * @return boolean
     */
    public function isConnected()
    {
        return $this->_connected;
    }

    /**
     * Returns the collection object, or null if this is disconnected rowset
     *
     * @return Db_Mongodb_Collection_Abstract
     */
    public function getCollection()
    {
        return $this->_collection;
    }

    /**
     * Set the collection object, to re-establish a live connection
     * to the database for a Rowset that has been de-serialized.
     *
     * @param Db_Mongodb_Collection_Abstract $collection
     * @return boolean
     * @throws Db_Mongodb_Collection_Document_Exception
     */
    public function setCollection(Db_Mongodb_Collection_Abstract $collection)
    {
        $this->_collection = $collection;
        $this->_connected = false;
        // @todo This works only if we have iterated through
        // the result set once to instantiate the rows.
        foreach ($this as $document) {
            $connected = $document->setCollection($collection);
            if ($connected == true) {
                $this->_connected = true;
            }
        }
        return $this->_connected;
    }

    /**
     * Query the class name of the collection object for which this
     * Rowset was created.
     *
     * @return string
     */
    public function getCollectionClass()
    {
        return $this->_collectionClass;
    }

    /**
     * Rewind the Iterator to the first element.
     * Similar to the reset() function for arrays in PHP.
     * Required by interface Iterator.
     *
     * @return Db_Mongodb_Collection_Documentset_Abstract Fluent interface.
     */
    public function rewind()
    {
        $this->_pointer = 0;
        return $this;
    }

    /**
     * Return the current element.
     * Similar to the current() function for arrays in PHP
     * Required by interface Iterator.
     *
     * @return Db_Mongodb_Collection_Document current element from the collection
     */
    public function current()
    {
        if ($this->valid() === false) {
            return null;
        }

        // do we already have a row object for this position?
        if (empty($this->_documents[$this->_pointer])) {
            $this->_documents[$this->_pointer] = new $this->_documentClass(
                array(
                    'collection'    => $this->_collection,
                    'data'             => $this->_data[$this->_pointer],
                    'stored'           => $this->_stored,
                    'readOnly'         => $this->_readOnly
                )
            );
        }

        // return the row object
        return $this->_documents[$this->_pointer];
    }

    /**
     * Return the identifying key of the current element.
     * Similar to the key() function for arrays in PHP.
     * Required by interface Iterator.
     *
     * @return int
     */
    public function key()
    {
        return $this->_pointer;
    }

    /**
     * Move forward to next element.
     * Similar to the next() function for arrays in PHP.
     * Required by interface Iterator.
     *
     * @return void
     */
    public function next()
    {
        ++$this->_pointer;
    }

    /**
     * Check if there is a current element after calls to rewind() or next().
     * Used to check if we've iterated to the end of the collection.
     * Required by interface Iterator.
     *
     * @return bool False if there's nothing more to iterate over
     */
    public function valid()
    {
        return $this->_pointer < $this->_count;
    }

    /**
     * Returns the number of elements in the collection.
     *
     * Implements Councollection::count()
     *
     * @return int
     */
    public function count()
    {
        return $this->_count;
    }

    /**
     * Take the Iterator to position $position
     * Required by interface SeekableIterator.
     *
     * @param int $position the position to seek to
     * @return Db_Mongodb_Collection_Documentset_Abstract
     * @throws Db_Mongodb_Collection_Documentset_Exception
     */
    public function seek($position)
    {
        $position = (int) $position;
        if ($position < 0 || $position >= $this->_count) {
            require_once 'Db/Mongodb/Collection/Exception.php';
            throw new Db_Mongodb_Collection_Exception("Illegal index $position");
        }
        $this->_pointer = $position;
        return $this;
    }

    /**
     * Check if an offset exists
     * Required by the ArrayAccess implementation
     *
     * @param string $offset
     * @return boolean
     */
    public function offsetExists($offset)
    {
        return isset($this->_data[(int) $offset]);
    }

    /**
     * Get the row for the given offset
     * Required by the ArrayAccess implementation
     *
     * @param string $offset
     * @return Db_Mongodb_Collection_Document
     */
    public function offsetGet($offset)
    {
        $this->_pointer = (int) $offset;

        return $this->current();
    }

    /**
     * Does nothing
     * Required by the ArrayAccess implementation
     *
     * @param string $offset
     * @param mixed $value
     */
    public function offsetSet($offset, $value)
    {
    }

    /**
     * Does nothing
     * Required by the ArrayAccess implementation
     *
     * @param string $offset
     */
    public function offsetUnset($offset)
    {
    }

    /**
     * Returns a Db_Mongodb_Collection_Document from a known position into the Iterator
     *
     * @param int $position the position of the row expected
     * @param bool $seek wether or not seek the iterator to that position after
     * @return Db_Mongodb_Collection_Document
     * @throws Db_Mongodb_Collection_Documentset_Exception
     */
    public function getDocument($position, $seek = false)
    {
        $key = $this->key();
        try {
            $this->seek($position);
            $document = $this->current();
        } catch (Db_Mongodb_Collection_Documentset_Exception $e) {
            require_once 'Db/Mongodb/Collection/Exception.php';
            throw new Db_Mongodb_Collection_Exception('No row could be found at position ' . (int) $position, 0, $e);
        }
        if ($seek == false) {
            $this->seek($key);
        }
        return $document;
    }

    /**
     * Returns all data as an array.
     *
     * Updates the $_data property with current row object values.
     *
     * @return array
     */
    public function toArray()
    {
        // @todo This works only if we have iterated through
        // the result set once to instantiate the rows.
        foreach ($this->_documents as $i => $document) {
            $this->_data[$i] = $document->toArray();
        }
        return $this->_data;
    }

}
