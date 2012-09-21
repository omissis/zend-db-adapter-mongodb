<?php
class Db_Mongodb_Collection extends Db_Mongodb_Collection_Abstract
{
    /**
     * A generic concrete class if you do not want to create individual models for each Mongodb collection.
     *
     * @param string $collection
     * @param mixed $config Array of user-specified config options, or just the Mongodb Adapter.
     */
    public function __construct($collection, $config)
    {
        /**
         * Allow a scalar argument to be the Adapter object or Registry key.
         */
        if (!is_array($config)) {
            $config = array(self::ADAPTER => $config);
        }

        if ($config) {
            $this->setOptions($config);
        }
        $this->_name = $collection;

        $this->_setup();
        $this->init();
    }
}