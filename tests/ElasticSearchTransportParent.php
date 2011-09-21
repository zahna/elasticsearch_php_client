<?php
require_once 'PHPUnit/Framework/TestCase.php';
require_once 'helper.php';
abstract class ElasticSearchTransportParent extends PHPUnit_Framework_TestCase
{
    protected $transport = null;

    /**
     * Test index creation
     */
    public function testCreateIndex() {
        $this->transport->createIndex(
                'created-index',
                $settings = array(
                    'number_of_shards' => 2,
				));
		$this->transport->setIndex('created-index');
		$result = $this->transport->request('_settings');
		$tmp = $result['created-index']['settings']['index.number_of_shards'];
		$this->transport->request('', 'DELETE');
		$this->assertTrue($tmp == 2);
    }

}
?>
