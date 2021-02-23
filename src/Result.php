<?php

namespace thedataist\Drill;

/**
 * @package Drill
 * @author Charles Givre <cgivre@thedataist.com>
 */
class Result {
	protected $columns;
	protected $rows;
	protected $query;
	protected $row_pointer;
	protected $metadata;
	protected $schema = array();

	/**
	 * Cleans the data types and specifically removes precision information
	 * from VARCHAR and DECIMAL data types which is not useful for UI work.
	 *
	 * @param string $dataType The string data type which should be a Drill MinorType
	 *
	 * @return string The datatype without precision information
	 */
	static function clean_data_type_name(string $dataType): string {
		$pattern = "/[a-zA-Z]+\(\d+(,\s*\d+)?\)/";
		if (preg_match($pattern, $dataType)) {
			$parts = explode('(', $dataType);
			$clean_data_type = $parts[0];
		} else {
			$clean_data_type = $dataType;
		}

		return $clean_data_type;
	}

	/**
	 * Result constructor.
	 *
	 * @param $response
	 * @param $query
	 */
	function __construct($response, $query) {
		$this->columns = $response['columns'];
		$this->rows = $response['rows'];
		$this->metadata = $response['metadata'];
		$this->query = $query;
		$this->row_pointer = 0;

		for ($i = 0; $i < count($this->columns); $i++) {
			$info = [];
			$info['column'] = $this->columns[$i];
			$info['data_type'] = self::clean_data_type_name($this->metadata[$i]);
			array_push($this->schema, $info);
		}
	}

	function data_seek($n) {
		if (!is_int($n)) {
			return false;
		} elseif ($n > count($this->rows)) {
			return false;
		} else {
			$this->row_pointer = $n;
			return true;
		}
	}

	function fetch_all() {
		return $this->rows;
	}

	function fetch_assoc() {
		if ($this->row_pointer >= count($this->rows)) {
			return false;
		} else {
			$result = $this->rows[$this->row_pointer];
			$this->row_pointer++;
			return $result;
		}
	}

	function get_schema() {
		return $this->schema;
	}

	function fetch_object() {
		if ($this->row_pointer >= count($this->rows)) {
			return false;
		} else {
			$result = $this->rows[$this->row_pointer];
			$result_object = new \stdClass();
			foreach ($result as $key => $value) {
				$result_object->$key = $value;
			}
			$this->row_pointer++;
			return $result_object;
		}
	}

	/**
	 * Fetch column names from results
	 *
	 * @return array
	 */
	function fetch_columns(): array {
		return $this->columns ? $this->columns : array();
	}

	/**
	 * Get number of fields
	 *
	 * @return int Number of fields
	 */
	function field_count(): int {
		return count($this->columns);
	}

	function more_results() {
		return $this->row_pointer < count($this->rows);
	}

	/**
	 * Retrieve the number of resulting rows
	 *
	 * @return int Number of Rows
	 */
	function num_rows(): int {
		return count($this->rows);
	}
}