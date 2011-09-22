<?php // vim:set ts=4 sw=4 et:
require_once 'helper.php';
class ElasticSearchMemcachedTransportTest extends TransportParent
{
    public function setUp()
    {
        $this->transport = new ElasticSearchTransportMemcached();
    }

    public function tearDown()
    {
        $this->transport = null;
    }
}
?>
