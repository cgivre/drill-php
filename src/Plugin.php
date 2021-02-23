<?php

namespace thedataist\Drill;

/**
 * Class Plugin
 * @package thedataist\Drill
 * @author Tim Swagger <tim@renowne.com>
 */
class Plugin {

	// region Properties
	/**
	 * Plugin Name
	 * @var string $name
	 */
	public string $name;

	/**
	 * List of Schemas for the current Plugin
	 * @var Schema[] $schemas
	 */
	public array $schemas = array();
	// endregion

	/**
	 * Schema constructor.
	 * @param object|array $data
	 */
	public function __construct($data = null) {
		$data = (object)$data;

		if(isset($data->name)) {
			$this->name = $data->name;
		}
		if(isset($data->schemas)) {
			$this->schemas = $data->schemas;
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