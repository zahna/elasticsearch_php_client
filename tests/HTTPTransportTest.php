<?php // vim:set ts=4 sw=4 et:
require_once 'helper.php';
class ElasticSearchHTTPTransportTest extends ElasticSearchTransportParent
{
    public static $host = 'localhost';
    public static $port = 9200;

    public function setUp()
    {
        $this->transport = new ElasticSearchTransportHTTP(
                self::$host,
                self::$port);
    }

    public function tearDown()
    {
        $this->transport = null;
    }
}
?>
