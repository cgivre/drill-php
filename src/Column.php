<?php

namespace thedataist\Drill;

/**
 * Class Column
 * @package thedataist\Drill
 * @author Tim Swagger <tim@renowne.com>
 */
class Column {
	// region Properties

	/**
	 * Plugin Name
	 * @var string $plugin
	 */
	public string $plugin;

	/**
	 * Schema Name
	 * @var string $schema
	 */
	public string $schema;

	/**
	 * Table Name
	 * @var string $table_name
	 */
	public string $table_name;

	/**
	 * Column Name
	 * @var string $name
	 */
	public string $name;

	/**
	 * Data type
	 * @var string $data_type
	 */
	public string $data_type;

	/**
	 * Nullable
	 * @var bool $is_nullable
	 */
	public bool $is_nullable;

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
		if(isset($data->schema)) {
			$this->schema = $data->schema;
		}
		if(isset($data->table_name)) {
			$this->table_name = $data->table_name;
		}
		if(isset($data->name)) {
			$this->name = $data->name;
		}
		if(isset($data->data_type)) {
			$this->data_type = $data->data_type;
		}
		if(isset($data->is_nullable)) {
			$this->is_nullable = $data->is_nullable === true || $data->is_nullable === 'YES';
		}
	}
}