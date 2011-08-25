<?php

abstract class ElasticSearchTransport {
    protected $index, $type;

    abstract public function index($document,
	    				$id = false,
					array $params = array());
    abstract public function request($path,
	    				$method = "GET",
					$payload = false,
					array $params = array());
    abstract public function delete($id = false, array $params = array());
    abstract public function search($query, array $params = array());
    abstract public function bulk($bulk_queue);

    public function setIndex($index) {
        $this->index = $index;
    }
    public function setType($type) {
        $this->type = $type;
    }

    /**
     * Build a callable url
     *
     * @return string
     * @param array $path
     * @param array $params Miscellaneous parameters to pass
     */
    protected function buildUrl($path=false, array $params = array()) {
        $url = "/" . $this->index;
        if ($path && count($path) > 0)
            $url .= "/" . implode("/", array_filter($path));
        if (substr($url, -1) == "/")
            $url = substr($url, 0, -1);
	if (count($params) > 0) {
	    if (strpos($url, '?')) {
		$url .= '&';
	    } else {
	        $url .= "?";
	    }
	    $url .= http_build_query($params);
	}
        return $url;
    }
}
