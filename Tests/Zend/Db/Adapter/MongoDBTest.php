<?php

class Zend_Db_Adapter_MongoDBTest extends \PHPUnit_Framework_TestCase
{
    /**
     * @expectedException Zend_Db_Adapter_Mongodb_Exception
     * @expectedExceptionMessage Adapter parameters must be in an array or a Zend_Config object
     */
    public function testInitWithoutZendConfigOrArray()
    {
        new Zend_Db_Adapter_MongoDB(null);
    }
}