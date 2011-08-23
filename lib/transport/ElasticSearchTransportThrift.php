<?php
require_once $GLOBALS['THRIFT_ROOT'].'/Thrift.php';
require_once $GLOBALS['THRIFT_ROOT'].'/protocol/TBinaryProtocol.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TSocket.php';
require_once $GLOBALS['THRIFT_ROOT'].'/transport/TBufferedTransport.php';

require_once $GLOBALS['THRIFT_ROOT'].'/packages/elasticsearch/Rest.php';

class ElasticSearchTransportThriftException extends ElasticSearchException {
	protected $data = array(
							'payload' => null,
							'port' => null,
							'host' => null,
							'url' => null,
							'method' => null,
							'params' => null
							);
	public function __set($key, $value) {
		if (array_key_exists($key, $this->data))
			$this->data[$key] = $value;
	}
	public function __get($key) {
		if (array_key_exists($key, $this->data))
			return $this->data[$key];
		else
			return false;
	}

	public function getCLICommand() {
		$postData = json_encode($this->payload);
		$paramString = (count($params) > 0) ? http_build_query($params) : '';
		$curlCall = "curl -X{$method} 'http://{$this->host}:{$this->port}{$this->url}?{$paramString}' -d '$postData'";
		return $curlCall;
	}
}

class ElasticSearchTransportThrift extends ElasticSearchTransport
{
	protected $host = "";
	protected $port = 0;

	protected $socket = NULL;
	protected $transport = NULL;
	protected $protocol = NULL;
	protected $client = NULL;


	public function __construct($host, $port) {
		try {
			$this->host = $host;
			$this->port = $port;

			$this->socket = new TSocket($host, $port);
			$this->transport = new TBufferedTransport($this->socket);
			$this->protocol = new TBinaryProtocol($this->transport);
			$this->client = new RestClient($this->protocol);
			$this->transport->open();
		} catch (Exception $e) {
			$msg = 'Error while attempting to open Thrift ';
			$msg .= 'connection to ElasticSearch: ';
			$msg .= "$e";
			throw new ElasticSearchTransportThriftException($msg);
		}
	}

	public function __destruct() {
		$this->transport->close();
	}

	/**
	 * Index a new document or update it if existing
	 *
	 * @return array
	 * @param array $document
	 * @param mixed $id Optional
	 */
	public function index($document, $id=false, array $params = array()) {
		$url = $this->buildUrl(array($this->type, $id));
		$method = ($id == false) ? "POST" : "PUT";
		try {
			$response = $this->call($url, $method, $document,
						$params);
		} catch (Exception $e) {
			$msg = 'Error while attempting to index: ';
			$msg .= "$e";
			throw new ElasticSearchTransportThriftException($msg);
		}

		return $response;
	}

	/**
	 * Search
	 *
	 * @return array
	 * @param mixed $id Optional
	 */
	public function search($query, array $params = array()) {
		if (is_array($query)) {
			/**
			 * Array implies using the JSON query DSL
			 */
			$url = $this->buildUrl(array($this->type, "_search"));
			try {
				$result = $this->call($url, "GET", $query,
							$params);
			} catch (Exception $e) {
				$msg = 'Error while attempting to search: ';
				$msg .= "$e";
				throw new ElasticSearchTransportThriftException($msg);
			}
		} else if (is_string($query)) {
			/**
			 * String based search means http query string search
			 */
			$url = $this->buildUrl(array($this->type, "_search"));
			$params['q'] = $query;
			$result = $this->call($url, "GET", null, $params);
			try {
				$result = $this->call($url, "GET", null,
							$params);
			} catch (Exception $e) {
				$msg = 'Error while attempting to search: ';
				$msg .= "$e";
				throw new ElasticSearchTransportThriftException($msg);
			}
		}
		return $result;
	}

	/**
	 * Search
	 *
	 * @return array
	 * @param mixed $id Optional
	 */
	public function deleteByQuery($query, array $params = array()) {
		if (is_array($query)) {
			/**
			 * Array implies using the JSON query DSL
			 */
			$url = $this->buildUrl(array($this->type, "_query"));
			try {
				$result = $this->call($url, "DELETE", $query,
							$params);
			} catch (Exception $e) {
				$msg = 'Error while attempting to delete by ';
				$msg .= 'query: ';
				$msg .= "$e";
				throw new ElasticSearchTransportThriftException($msg);
			}
		} else if (is_string($query)) {
			/**
			 * String based search means http query string search
			 */
			$url = $this->buildUrl(array($this->type, "_query"));
			$params['q'] = $query;
			try {
				$result = $this->call($url, "DELETE", null,
							$params);
			} catch (Exception $e) {
				$msg = 'Error while attempting to delete by ';
				$msg .= 'query: ';
				$msg .= "$e";
				throw new ElasticSearchTransportThriftException($msg);
			}
		}
		return $result['ok'];
	}

	/**
	 * Basic http call
	 *
	 * @return array
	 * @param mixed $id Optional
	 */
	public function request($path, $method="GET", $payload = null,
				array $params = array()) {
		$url = $this->buildUrl($path);
		try {
			$result = $this->call($url, $method, $payload,
						$params);
		} catch (Exception $e) {
			$msg = 'Error while attempting to perform request: ';
			$msg .= "$e";
			throw new ElasticSearchTransportThriftException($msg);
		}
		return $result;
	}

	/**
	 * Flush this index/type combination
	 *
	 * @return array
	 */
	public function delete($id=false, array $params = array()) {
		if ($id)
			return $this->request(array($this->type, $id),
						"DELETE", null, $params);
		else
			return $this->request(false, "DELETE", null, $params);
	}

	/**
	 * Perform a http call against an url with an optional payload
	 *
	 * @return array
	 * @param string $url
	 * @param string $method (GET/POST/PUT/DELETE)
	 * @param array $payload The document/instructions to pass along
	 */
	protected function call($url, $method="GET", $payload=false,
				array $params = array()) {
		$req = array("method" => $GLOBALS['E_Method'][$method],
				"uri" => $url,
				'parameters' => $params);

		if (is_array($payload) && count($payload) > 0) {
			$req["body"] = json_encode($payload);
		}

		$result = $this->client->execute(new RestRequest($req));

		if (!isset($result->status)){
			$exception = new ElasticSearchTransportThriftException();
			$exception->payload = $payload;
			$exception->port = $this->port;
			$exception->host = $this->host;
			$exception->method = $method;
			throw $exception;
		}

		$data = json_decode($result->body, true);

		if (array_key_exists('error', $data)) {
			$this->handleError($url, $method, $payload, $data,
						$params);
		}

		return $data;
	}

	protected function handleError($url, $method, $payload, $response,
					array $params = array()) {
		$err = "Request: \n";
		$err .= "curl -X$method http://{$this->host}:{$this->port}$url";
		if (count($params)>0) $err .= http_build_query($params);
		if ($payload) $err .=  " -d '" . json_encode($payload) . "'";
		$err .= "\n";
		$err .= "Triggered some error: \n";
		$err .= $response['error'] . "\n";
		//echo $err;
	}

	/**
	 * Build a callable url
	 *
	 * @return string
	 * @param array $path
	 */
	protected function buildUrl($path=false) {
		$url = "/" . $this->index;
		if ($path && count($path) > 0)
			$url .= "/" . implode("/", array_filter($path));
		if (substr($url, -1) == "/")
			$url = substr($url, 0, -1);
		return $url;
	}
}
