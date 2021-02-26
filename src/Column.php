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
	 * Column Name
	 * @var string $name
	 */
	public string $name;

	/**
	 * Data type
	 * @var string $dataType
	 */
	public string $dataType;

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
		if(isset($data->dataType)) {
			$this->dataType = $data->dataType;
		}
	}
}