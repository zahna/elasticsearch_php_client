<?php

class ElasticSearchBulkQueue
{
	private $_bulk_queue = array();
	private $_params = array();

	public function setParams(array $params) {
		foreach ($params as $key => $value) {
			$this->_params[$key] = $value;
		}
	}

	public function getParams() {
		return $this->_params;
	}

	public function getHTTPParamString() {
		return http_build_query($this->_params);
	}

	public function index($document,
				$index,
				$type,
				$id = false,
				array $metadata = array()) {

		if (($index == null) || ($type == null)) {
			$msg = "An index and a type must be specified in ";
			$msg .= "order to index a document.";
			throw new ElasticSearchException($msg);
		}

		$metadata['_index'] = $index;
		$metadata['_type'] = $type;
		if ($id) {
			$metadata['_id'] = $id;
		}

		$this->_bulk_array[] = array('action' => 'index',
						'metadata' => $metadata,
						'document' => $document);

	}

	public function delete($index,
				$type,
				$id,
				array $metadata = array()) {

		if (($index == null) || ($type == null) || ($id == null)) {
			$msg = "An index, a type, and an ID must be specified";
			$msg .= " in order to delete a document.";
			throw new ElasticSearchException($msg);
		}

		$metadata['_index'] = $index;
		$metadata['_type'] = $type;
		$metadata['_id'] = $id;

		$this->_bulk_array[] = array('action' => 'delete',
						'metadata' => $metadata);
	}

	public function getPayload() {
		$bulk_doc = '';

		foreach ($this->_bulk_array as $bulk_item) {
			$bulk_doc .= '{"';
			$bulk_doc .= $bulk_item['action'];
			$bulk_doc .= '":{';

			$metadata = array();
			foreach ($bulk_item['metadata'] as $var => $val) {
				$metadata[] = '"'.$var.'":"'.$val.'"';
			}

			$bulk_doc .= implode(',', $metadata);
			$bulk_doc .= "}}\n";
			if ($bulk_item['action'] == 'index') {
				$bulk_doc .=
					json_encode($bulk_item['document'],
							JSON_FORCE_OBJECT);
				$bulk_doc .= "\n";
			}
		}

		return $bulk_doc;
	}
}

?>
