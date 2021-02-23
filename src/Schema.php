<?php


namespace thedataist\Drill;

/**
 * Class Schema
 * @package thedataist\Drill
 * @author Tim Swagger <tim@renowne.com>
 */
class Schema {

	// region Properties
	/**
	 * Name of parent plugin/dataSource
	 * @var string $plugin
	 */
	public string $plugin;

	/**
	 * Name of Schema
	 * @var string $name
	 */
	public string $name;

	/**
	 * List of tables in schema
	 * @var array $tables
	 */
	public array $tables = array();
	// endregion

	/**
	 * Schema constructor.
	 * @param object|array $data
	 */
	public function __construct($data = null) {
		$data = (object)$data;

		if(isset($data->plugin)) {
			$this->plugin = $data->plugin;
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