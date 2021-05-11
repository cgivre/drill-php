<?php

namespace thedataist\Drill;

/**
 * Class Table
 * @package thedataist\Drill
 * @author Tim Swagger <tim@renowne.com>
 */
class Table {

	// region Properties
	/**
	 * Name of parent schema/database
	 * @var string $schema
	 */
	public string $schema;

	/**
	 * Name of Table
	 * @var string $name
	 */
	public string $name;

	/**
	 * List of columns in table
	 * @var Column[] $columns
	 */
	public array $columns = array();
	// endregion

	/**
	 * Schema constructor.
	 * @param object|array $data
	 */
	public function __construct($data = null) {
		$data = (object)$data;

		if(isset($data->schema)) {
			$this->schema = $data->schema;
		}
		if(isset($data->name)) {
			$this->name = $data->name;
		}
	}

	/**
	 * Magic method setter
	 *
	 * @param string $name Property name
	 * @param mixed $value property value
	 */
	public function __set(string $name, $value): void {
		$this->$name = $value;
	}

	/**
	 * Magic method getter
	 *
	 * @param string $name Property Name
	 * @return mixed Property Value
	 */
	public function __get(string $name) {
		return $this->$name;
	}
}