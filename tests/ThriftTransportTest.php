<?php // vim:set ts=4 sw=4 et:
require_once 'helper.php';
class ElasticSearchThriftTransportTest extends ElasticSearchTransportParent
{
    public static $host = 'localhost';
    public static $port = 9520;

    public function setUp()
    {
        $this->transport = new ElasticSearchTransportThrift(
                self::$host,
                self::$port);
    }
    public function tearDown()
    {
       $this->transport = null;
    }
}
?>
