<?php
class Db_Mongo
{
    protected static $_instance;
    public function __construct(array $dbparams = null)
    {

    }
    public static function getInstance()
    {
        if (empty(self::$_instance) || !self::$_instance instanceof Db_Mongo) {
            self::$_instance = new self();
        }
    }
}