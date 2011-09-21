<?php // vim:set ts=4 sw=4 et:
require_once 'helper.php';
class ElasticSearchThriftTest extends ElasticSearchParent {

    public static $host = 'localhost';
    public static $port = 9520;
    public static $bad_port = 1337;

    public function setUp() {
        $transport = new ElasticSearchTransportThrift(self::$host, self::$port);
        $this->search = new ElasticSearchClient($transport, "test-index", "test-type");
        $this->search->delete();
    }
    public function tearDown() {
        $this->search->delete();
        $this->search = null;
    }

    /**
     * Test indexing a new document and having an auto id
     * This means dupes will occur
     */
    public function testIndexingDocumentWithoutId() {
        $doc = array(
            'title' => 'One cool document',
            'tag' => 'cool'
        );
        $resp = $this->search->index($doc);
        $this->assertTrue($resp['ok'] == 1);
    }

    public function testBulkIndexingDocumentWithoutId() {
        $doc = array(
            'title' => 'One cool document',
            'tag' => 'cool'
        );
        $this->search->bulkIndex($doc);

        $options = array('refresh' => true);
        $resp = $this->search->bulkSubmit($options);

        $this->assertTrue($resp['items'][0]['create']['ok'] == 1);
    }

    /**
     * Test delete by query
     */
    public function testDeleteByQuery() {
        $doc = array('title' => 'not cool yo');
        $this->search->setIndex("test-index");
        $this->search->index($doc, 1);

        sleep(1); // To make sure the documents will be ready

        $del = $this->search->deleteByQuery(array(
            'term' => array('title' => 'cool')
        ));

        $this->assertTrue($del);

        sleep(1); // To make sure the documents will be ready

        // Use both indexes when searching
        $hits = $this->search->search(array(
            'query' => array(
                'term' => array('title' => 'cool')
            )
        ));
        $this->assertEquals(0, $hits['hits']['total']);
    }

    /**
     * Test a midly complex search
     */
    public function testSlightlyComplexSearch() {
        $this->addDocuments();
        $doc = array(
            'title' => 'One cool document',
            'body' => 'Lorem ipsum dolor sit amet',
            'tag' => array('cool', "stuff", "2k")
        );
        $resp = $this->search->index($doc, 1);
        sleep(1); // Indexing is only near real time

        $hits = $this->search->search(array(
            'query' => array(
                'bool' => array(
                    'must' => array(
                        'term' => array('title' => 'cool')
                    ),
                    'should' => array(
                        'field' => array(
                            'tag' => 'stuff'
                        )
                    )
                )
            )
        ));
        $this->assertEquals(3, $hits['hits']['total']);
    }

    /**
     * Test a midly complex search with bulk
     */
    public function testSlightlyComplexSearchBulk() {
        $this->addDocumentsBulk();
        $doc = array(
            'title' => 'One cool document',
            'body' => 'Lorem ipsum dolor sit amet',
            'tag' => array('cool', "stuff", "2k")
        );
        $resp = $this->search->index($doc, 1);
        sleep(1); // Indexing is only near real time

        $hits = $this->search->search(array(
            'query' => array(
                'bool' => array(
                    'must' => array(
                        'term' => array('title' => 'cool')
                    ),
                    'should' => array(
                        'field' => array(
                            'tag' => 'stuff'
                        )
                    )
                )
            )
        ));
        $this->assertEquals(3, $hits['hits']['total']);
    }

    /**
     * @expectedException ElasticSearchTransportThriftException
     */
    public function testSearchThrowExceptionWhenServerDown() {
        $transport = new ElasticSearchTransportThrift(self::$host, self::$bad_port);
        $search = new ElasticSearchClient($transport, "test-index", "test-type");
        $search->search("title:cool");
    }
}
?>
