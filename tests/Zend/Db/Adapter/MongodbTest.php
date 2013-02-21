<?php

class Zend_Db_Adapter_MongodbTest extends \PHPUnit_Framework_TestCase
{
    protected $parameters = array();

    public function __construct()
    {
        $this->parameters = parse_ini_file(__DIR__ . '/_files/parameters.ini', true);
    }

    public function tearDown()
    {
        $adapter = $this->createAdapter();

        $connection = $adapter->getConnection();

        $db = $connection->selectDB($this->parameters['mongodb']['db']);

        $db->drop();
    }

    /**
     * @expectedException PHPUnit_Framework_Error
     */
    public function testInitWithoutArray()
    {
        new Zend_Db_Adapter_Mongodb(null);
    }

    /**
     * @expectedException Zend_Db_Adapter_Mongodb_Exception
     * @expectedExceptionMessage Configuration array must have a key for 'db' that names the database instance
     */
    public function testInitWithoutDbName()
    {
        new Zend_Db_Adapter_Mongodb(array());
    }

    /**
     * @expectedException Zend_Db_Adapter_Mongodb_Exception
     * @expectedExceptionMessage Configuration array must have a key for 'password' for login credentials
     */
    public function testInitWithoutPassword()
    {
        new Zend_Db_Adapter_Mongodb(array(
            'db' => $this->parameters['mongodb']['db'],
        ));
    }

    /**
     * @expectedException Zend_Db_Adapter_Mongodb_Exception
     * @expectedExceptionMessage Configuration array must have a key for 'username' for login credentials
     */
    public function testInitWithoutUsername()
    {
        new Zend_Db_Adapter_Mongodb(array(
            'db'   => $this->parameters['mongodb']['db'],
            'password' => $this->parameters['mongodb']['password'],
        ));
    }

    /**
     * @expectedException Zend_Db_Adapter_Mongodb_Exception
     * @expectedExceptionMessage Configuration array must have a key for 'host'
     */
    public function testInitWithoutHost()
    {
        new Zend_Db_Adapter_Mongodb(array(
            'db'   => $this->parameters['mongodb']['db'],
            'password' => $this->parameters['mongodb']['password'],
            'username' => $this->parameters['mongodb']['username'],
        ));
    }

    /**
     * @expectedException Zend_Db_Adapter_Mongodb_Exception
     * @expectedExceptionMessage Configuration array must have a key for 'port'
     */
    public function testInitWithoutHostPort()
    {
        new Zend_Db_Adapter_Mongodb(array(
            'db'   => $this->parameters['mongodb']['db'],
            'password' => $this->parameters['mongodb']['password'],
            'username' => $this->parameters['mongodb']['username'],
            'host'     => $this->parameters['mongodb']['host'],
        ));
    }

    public function testCompleteInit()
    {
        $adapter = $this->createAdapter();

        $this->assertSame($this->parameters['mongodb']['db'], $adapter->getDbName());
        $this->assertSame($this->parameters['mongodb']['password'], $adapter->getPassword());
        $this->assertSame($this->parameters['mongodb']['username'], $adapter->getUsername());
        $this->assertSame($this->parameters['mongodb']['host'], $adapter->getHost());
        $this->assertSame($this->parameters['mongodb']['port'], $adapter->getPort());
    }

    /**
     * @expectedException MongoConnectionException
     * @expectedExceptionMessage Couldn't get host info for quux
     */
    public function testInitWithoWrongConfig()
    {
        $adapter = new Zend_Db_Adapter_Mongodb(array(
            'db'   => 'foo',
            'password' => 'bar',
            'username' => 'baz',
            'host'     => 'quux',
            'port'     => 123,
        ));

        $this->assertSame('foo', $adapter->getDbName());
        $this->assertSame('bar', $adapter->getPassword());
        $this->assertSame('baz', $adapter->getUsername());
        $this->assertSame('quux', $adapter->getHost());
        $this->assertSame(123, $adapter->getPort());
    }

    public function testGetMongoDBConnection()
    {
        $adapter = $this->createAdapter();

        if (class_exists('\MongoClient')) {
            $this->assertInstanceOf('\MongoClient', $adapter->getConnection());
        } else {
            $this->assertInstanceOf('\Mongo', $adapter->getConnection());
        }
        $this->assertInstanceOf('\MongoDB', $adapter->setUpDatabase());
        $this->assertInstanceOf('\MongoDB', $adapter->getMongoDB());

        $this->assertInstanceOf('\MongoDB', $adapter->setUpDatabase('foobar'));
    }

    public function testMagicMethodCallWithoutDBInit()
    {
        $adapter = $this->createAdapter();

        $this->assertInternalType('array', $adapter->listCollections());
    }

    /**
     * @expectedException Zend_Db_Adapter_Mongodb_Exception
     * @expectedExceptionMessage MongoDB::fooBar Method not found
     */
    public function testMagicMethodCallWithDBInitAndWrongMethod()
    {
        $adapter = $this->createAdapter();

        $this->assertInternalType('array', $adapter->fooBar());
    }

    public function testQueryWithDBInit()
    {
        $adapter = $this->createAdapter();

        $results = $adapter->query('return db.foo.count();');

        $this->assertInternalType('array', $results);
        $this->assertArrayHasKey('retval', $results);
        $this->assertEquals(0.0, $results['retval']);
        $this->assertArrayHasKey('ok', $results);
        $this->assertEquals(1.0, $results['ok']);
    }

    protected function createAdapter()
    {
        return new Zend_Db_Adapter_Mongodb(array(
            'db'   => $this->parameters['mongodb']['db'],
            'password' => $this->parameters['mongodb']['password'],
            'username' => $this->parameters['mongodb']['username'],
            'host'     => $this->parameters['mongodb']['host'],
            'port'     => $this->parameters['mongodb']['port'],
        ));
    }
}
