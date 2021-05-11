<?php

namespace thedataist\Drill;

use phpDocumentor\Reflection\Project;

/**
 * @package Drill
 * @author Charles Givre <cgivre@thedataist.com>
 */
class DrillConnection {

	// region Properties
	/**
	 * Hostname
	 * @var string $hostname
	 */
	protected string $hostname;

	/**
	 * Port number
	 * @var int $port
	 */
	protected $port;

	/**
	 * User name
	 * @var string $username
	 */
	protected $username;

	/**
	 * Password
	 * @var string $password
	 */
	protected $password;

	/**
	 * Use SSH
	 * @var bool $ssl
	 */
	protected $ssl = false;

	/**
	 * Error Messages
	 * @var ?String $error_message
	 */
	protected $error_message = null;

	/**
	 * Columns
	 * @var ?array $columns;
	 */
	protected $columns = null;

	/**
	 * Rows
	 * @var ?array $rows
	 */
	protected $rows = null;

	/**
	 * Cache of Plugins
	 * @var ?array $cached_plugins
	 */
	protected $cached_plugins = null;

	/**
	 * Default schema
	 * @var ?string $default_schema
	 */
	protected $default_schema = null;

	/**
	 * Row Limit
	 * @var int $row_limit
	 */
	protected $row_limit;

	/**
	 * Cache of enabled plugins
	 * @var ?array $cached_enabled_plugins
	 */
	protected $cached_enabled_plugins = null;

	// endregion
	// region Constructor

	/**
	 * DrillConnection constructor.
	 *
	 * @param string $host Drill instance Hostname
	 * @param int $arg_port Port Number
	 * @param string $username Username [default: '']
	 * @param string $password Password [default: '']
	 * @param bool $ssl Use SSL/TLS Connection [default: false]
	 * @param int $row_limit Row Limit [default: 10000]
	 */
	public function __construct(string $host, int $arg_port, string $username = '', string $password = '', bool $ssl = true, int $row_limit = 10000) {
		$this->hostname = $host;
		$this->port = $arg_port;
		$this->username = $username;
		$this->password = $password;
		$this->ssl = $ssl;
		$this->row_limit = $row_limit;
//		$this->cached_enabled_plugins = $this->get_enabled_storage_plugins();
	}

	// endregion

	// region Query Methods

	/**
	 * Checks if the connection is active.
	 *
	 * @return bool Returns true if the connection to Drill is active, false if not.
	 */
	public function is_active(): bool {
		$protocol = $this->ssl ? 'https://' : 'http://';

		$result = @get_headers($protocol.$this->hostname.':'.$this->port);

		return isset($result[1]);
	}

	/**
	 * Executes a Drill query.
	 *
	 * @param string $query The query to run/execute
	 *
	 * @return ?Result Returns Result object if the query executed successfully, null otherwise.
	 * @throws \Exception
	 */
	function query(string $query): ?Result {

		$url = $this->build_url('query');

		$postData = array(
			'queryType' => 'SQL',
			'query' => $query,
			'autoLimit' => $this->row_limit,
			'options' => [
				'drill.exec.http.rest.errors.verbose' => true
			]
		);

		$response = $this->post_request($url, $postData);

		if (isset($response['errorMessage'])) {
			$this->errorMessage = $response['errorMessage'];
			$this->stackTrace = $response['stackTrace'] ?? '';
			throw new \Exception("Error in query: {$query}");
		} else {
			return new Result($response, $query);
		}

	}

	// region Plugin Methods

	/**
	 * Identify plugin type
	 *
	 * @param ?string $plugin Plugin name
	 *
	 * @return ?string The plugin type, or null on error
	 */
	function get_plugin_type(?string $plugin): ?string {
		if (! isset($plugin) || !$this->is_active()) {
			return null;
		}

		// Remove back ticks
		$plugin = str_replace("`", "", $plugin);

		$query = "SELECT SCHEMA_NAME, TYPE FROM INFORMATION_SCHEMA.`SCHEMATA` WHERE SCHEMA_NAME LIKE '%$plugin%' LIMIT 1";

		// Should only be one row
		$info = $this->query($query)->fetch_assoc();

		if (! isset($info)) {
			return null;
		}

		return strtolower($info['TYPE']);
	}

	/**
	 * Disable Selected Plugin
	 *
	 * @param string $plugin Plugin name
	 * @return bool True if plugin successfully disabled, false otherwise.
	 */
	function disable_plugin(string $plugin): bool {
		$url = $this->build_url('disable_plugin', $plugin);
		$result = $this->get_request($url);

		if (isset($result['result']) && $result['result'] === 'success') {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * Enable Selected Plugin
	 *
	 * @param string $plugin Plugin name
	 * @return bool True if plugin successfully enabled, false otherwise.
	 */
	function enable_plugin(string $plugin): bool {
		$url = $this->build_url('enable_plugin', $plugin);
		$result = $this->get_request($url);

		if (isset($result['result']) && $result['result'] === 'success') {
			return true;
		} else {
			return false;
		}
	}

	/**
	 * This function returns an array of all storage plugins.
	 *
	 * @return array The list of all storage plugins, empty array if none
	 */
	function get_all_storage_plugins(): array {
		$plugin_info = $this->get_storage_plugins();
		$all_plugins = [];
		$enabled_plugins = [];

		foreach ($plugin_info as $plugin) {
			array_push($all_plugins, $plugin['name']);
			if ($plugin_info['config']['enabled'] == 1) {
				array_push($enabled_plugins, $plugin['name']);
			}
		}

		$this->cached_plugins = $all_plugins;
		$this->cached_enabled_plugins = $enabled_plugins;

		return $all_plugins;
	}

	/**
	 * Retrieves an associative array of storage plugins.
	 *
	 * It will have all configuration options for the plugins.
	 *
	 * @return array The list storage plugins as an associative array, empty array if none
	 */
	function get_storage_plugins(): array {

		$url = $this->build_url('storage');
		return $this->get_request($url);
	}

	/**
	 * Retrieves a list of enabled storage plugins.
	 *
	 * @return array A list of enabled storage plugins. Empty array if none.
	 */
	public function get_enabled_storage_plugins(): array {
		if (! $this->is_active()) {
			return array();
		}
		$plugin_info = $this->get_storage_plugins();
		$enabled_plugins = [];
		foreach ($plugin_info as $plugin) {
			if(isset($plugin['config']['enabled']) && $plugin['config']['enabled']) {
				$enabled_plugins[] = $plugin;
			}
		}
		$this->cached_enabled_plugins = $enabled_plugins;
		return $enabled_plugins;
	}

	/**
	 * Retrieves a list of storage plugins which are disabled.
	 *
	 * @return array List of disabled storage plugins, empty array if none
	 */
	function get_disabled_storage_plugins(): array {
		$plugin_info = $this->get_storage_plugins();
		$disabled_plugins = [];

		foreach ($plugin_info as $plugin) {
			if ($plugin['config']['enabled'] == 0) {
				array_push($disabled_plugins, $plugin['name']);
			}
		}

		return $disabled_plugins;
	}

	/**
	 * Retrieves the cached list of enabled plugins.
	 *
	 * Theoretically you can reduce API calls with this method.
	 *
	 * @return array The list storage plugins as an associative array, empty array if none.
	 */
	function get_cached_enabled_plugins(): array {

		if (! isset($this->cached_enabled_plugins)) {
			$this->cached_enabled_plugins = $this->get_enabled_storage_plugins();
		}

		return $this->cached_enabled_plugins;
	}

	/**
	 * Retrieves the cached list of plugins.
	 *
	 * Theoretically you can reduce API calls with this method.
	 *
	 * @return array The list storage plugins as an associative array, empty array if none.
	 */
	function get_cached_plugins(): array {

		if (! isset($this->cached_plugins)) {
			$this->cached_plugins = $this->get_storage_plugins();
		}

		return $this->cached_plugins;
	}

	/**
	 * This function returns an array of configuration options for a given storage plugin.
	 *
	 * @param string $plugin The plain text name of the storage plugin.
	 *
	 * @return array Array containing all configuration options for the given plugin
	 */
	function get_storage_plugin_info(string $plugin): array {

		$url = $this->build_url('plugin-info', $plugin);

		return $this->get_request($url);
	}

	// endregion

	// region Schema Methods
	/**
	 * Retrieves list of enabled schemata names
	 *
	 * @return ?array A list of enabled schemata, null on error
	 */
	function get_schema_names(): ?array {
		if (! $this->is_active()) {
			return null;
		}

		$query = 'SHOW DATABASES';
		$schemata = array();

		$raw_results = $this->query($query)->fetch_all();
		if (!$raw_results) {
			$this->error_message = 'Error retrieving schema names';
			return null;
		}

		// TODO: strip tables as well.
		foreach ($raw_results as $result) {
			$schema = $result['SCHEMA_NAME'];
			if ($schema != 'cp.default' &&
				$schema != 'INFORMATION_SCHEMA' &&
				$schema != 'information_schema' &&
				$schema != 'dfs.default' &&
				$schema != 'sys') {
				$schemata[] = $schema;
			}
		}

		return $schemata;
	}

	/**
	 * Retrieves an organized/tree listing of schema names
	 *
	 * @return ?Schema[] An organized list of schema names
	 */
	public function get_schema(): ?array {
		$nameList = $this->get_schema_names();
		if(! isset($nameList)) {
			// error getting schema names;
			return null;
		}

		$schemata = array();
		$hasDot = '/\./';

		foreach($nameList as &$name) {
			// Skip plugin entries, we will pick them up elsewhere
			if(! preg_match($hasDot, $name)) {
				continue;
			}

			$nameSplit = preg_split($hasDot, $name);
			if(! isset($schemata[$nameSplit[0]])) {
				// Create plugin entry if not exist
				$schemata[$nameSplit[0]] = new namespace\Plugin(['name'=>$nameSplit[0]]);
			}

			$schemata[$nameSplit[0]]->schemas[] = new namespace\Schema(['plugin'=>$nameSplit[0], 'name'=>$nameSplit[1]]);
		}

		return $schemata;
	}
	// endregion

	// region Table Methods

	/**
	 * Retrieves a list of Drill tables which exist in a Drill schema.
	 *
	 * For a file system, this is essentially a list of files in a given
	 * workspace.  For a relational database, this corresponds to the list
	 * of tables in the database.
	 *
	 * @param string $plugin The plain text name of the storage plugin.
	 * @param string $schema The input schema
	 *
	 * @return array List of table names
	 */
	function get_table_names(string $plugin, string $schema): array {
		$clean_plugin = str_replace('`', '', $plugin);
		$clean_schema = str_replace('`', '', $schema);
		$plugin_type = $this->get_plugin_type($clean_plugin);
		$table_names = array();

		$clean_schema = $clean_plugin . '.' . $clean_schema;

		if ($plugin_type === 'file') {
			$sql = "SELECT FILE_NAME FROM information_schema.`files` WHERE SCHEMA_NAME = '$clean_schema' AND IS_FILE = true";
			$tables = $this->query($sql)->fetch_all();

			foreach ($tables as $table) {
				if (strpos($table['FILE_NAME'], 'view.drill')) {
					// Skip views.  Use the get_view_names() method instead
					continue;
				} else {
					$table_names[] = $table['FILE_NAME'];
				}
			}
		} else {
			$sql = "SELECT `TABLE_NAME` FROM INFORMATION_SCHEMA.`TABLES` WHERE `TABLE_SCHEMA` = '$clean_schema'";
			$tables = $this->query($sql)->fetch_all();

			foreach ($tables as $table) {
				if (strpos($table['TABLE_NAME'], 'view.drill')) {
					$table_name = str_replace('view.drill', '', $table['TABLE_NAME']);
				} else {
					$table_name = $table['TABLE_NAME'];
				}
				$table_names[] = $table_name;
			}
		}

		return $table_names;
	}


	/**
	 * Retrieves an array of tables names
	 *
	 * @param string $plugin The plain text name of the storage plugin.
	 * @param string $schema The input schema
	 *
	 * @return Table[] List of tables. Empty array if there are none
	 */
	function get_tables(string $plugin, string $schema): array {
		$tables = array();

		foreach($this->get_table_names($plugin, $schema) as $table_name) {
			$tables[] = new namespace\Table(['name'=>$table_name, 'schema'=>$schema]);
		}

		return $tables;
	}

	/**
	 * Retrieve the View Names
	 *
	 * @param string $plugin Plugin/Datasource to retrieve view names from
	 * @param string $schema Schema to retrieve view names from
	 *
	 * @return ?array List of names or null if error
	 */
	function get_view_names(string $plugin, string $schema): ?array {
		if (!$this->is_active()) {
			return null;
		}

		$view_names = array();
		$sql = "SELECT `TABLE_NAME` FROM INFORMATION_SCHEMA.views WHERE table_schema='{$plugin}.{$schema}'";
		$results = $this->query($sql)->fetch_all();

		foreach ($results as $result) {
			$view_names[] = $result['TABLE_NAME'];
		}
		return $view_names;
	}

	// endregion

	// region Column Methods
	/**
	 * Retrieves the columns present in a given data source.
	 *
	 * Drill schema discovery behaves differently for the different plugin types.
	 * If the data is a file, API, MongoDB or Splunk, we have to execute a
	 * `SELECT *` query with a LIMIT 1 to identify the columns and data types.
	 *
	 * If the data is a database, we can use the DESCRIBE TABLE command to access schema information.
	 *
	 * @param string $plugin The plugin name
	 * @param string $schema The schema name
	 * @param string $table_name The table or file name
	 *
	 * @return Column[] List of columns present
	 */
	function get_columns(string $plugin, string $schema, string $table_name): array {

		$plugin_type = $this->get_plugin_type($plugin);

		// Since MongoDB uses the ** notation, bypass that and query the data directly
		// TODO: Add API functionality here as well
		if ($plugin_type === 'file' || $plugin_type === 'mongo' || $plugin_type === 'splunk') {

			$views = $this->get_view_names($plugin, $schema);

			$file_name = "{$plugin}.{$schema}.{$table_name}";

			if ($plugin_type === 'mongo') {
				$mongo_quoted_file_name = $this->format_drill_table($file_name, false);
				$sql = "SELECT ** FROM {$mongo_quoted_file_name} LIMIT 1";
			} else if (in_array($table_name, $views)) {
				$view_name = "`{$plugin}.{$schema}`.`{$table_name}`"; // NOTE: escape char ` may need to go around plugin and schema separately
				$sql = "SELECT * FROM {$view_name} LIMIT 1";
			} else if ($plugin_type === 'splunk') {
				// Case for Splunk
				$splunk_quoted_file_name = $this->format_drill_table($file_name, false);
				$sql = "SELECT * FROM {$splunk_quoted_file_name} LIMIT 1";
			} else {
				$quoted_file_name = $this->format_drill_table($file_name, true);
				$sql = "SELECT * FROM {$quoted_file_name} LIMIT 1";
			}

			// TODO: process this to return Column[]
			return $this->query($sql)->get_schema();

//		} else if (str_contains($table_name, "SELECT")) { // replaced with regex, str_contains is >=PHP8.0
		} else if (preg_match('/SELECT/', $table_name)) {

			$sql = "SELECT * FROM {$table_name} LIMIT 1";

		} else {
			/*
			 * Case for everything else.
			 */
			$quoted_schema = $this->format_drill_table("{$plugin}.{$schema}.{$table_name}", false);
			$sql = "DESCRIBE {$quoted_schema}";
		}

		$result = $this->query($sql)->fetch_all();

		$columns = array();
		foreach($result as $row) {
			$data = array(
				'plugin' => $plugin,
				'schema' => $schema,
				'table' => $table_name,
				'name' => $row['COLUMN_NAME'],
				'data_type' => $row['DATA_TYPE'],
				'is_nullable' => $row['IS_NULLABLE']
			);

			$columns[] = new namespace\Column($data);
		}
		return $columns;
	}
	// endregion

	// endregion

	// region Management Methods

	/**
	 * Retrieves the error message from the most recent query.
	 *
	 * @return string The error message from the most recent query, an empty string if undefined.
	 */
	function error_message(): string {
		return isset($this->error_message) ? $this->error_message : '';
	}

	/**
	 * Format Drill Table
	 *
	 * @param string $schema Schema name
	 * @param bool $is_file Schema/DB is a file
	 *
	 * @return string
	 */
	function format_drill_table(string $schema, bool $is_file): string {
		$formatted_schema = "";

		$num_dots = substr_count($schema, ".");
		$schema = str_replace('`', '', $schema);

		// For files, the last section will be the file extension
		$schema_parts = explode('.', $schema);

		if ($is_file && $num_dots == 3) {
			// Case for file and workspace
			$plugin = $schema_parts[0];
			$workspace = $schema_parts[1];
			$table = $schema_parts[2] . "." . $schema_parts[3];
			$formatted_schema = $plugin . ".`" . $workspace . "`.`" . $table . "`";
		} else if ($is_file && $num_dots == 2) {
			// Case for File and no workspace
			$plugin = $schema_parts[0];
			$formatted_schema = $plugin . ".`" . $schema_parts[1] . "." . $schema_parts[2] . "`";
		} else {
			// Case for everything else
			foreach ($schema_parts as $part) {
				$quoted_part = "`" . $part . "`";
				if (strlen($formatted_schema) > 0) {
					$formatted_schema = "$formatted_schema.$quoted_part";
				} else {
					$formatted_schema = $quoted_part;
				}
			}
		}
		return $formatted_schema;
	}

	// endregion
	// region Private Methods
	/**
	 * Build URL
	 *
	 * @param string $function The Function to be called [default: '']
	 * @param string $extra Any extra to be included [default: '']
	 *
	 * @return string The completed URL
	 */
	private function build_url($function = '', $extra = ''): string {

		$protocol = $this->ssl ? 'https://' : 'http://';

		switch($function) {
			case 'query':
				$path = '/query.json';
				break;
			case 'storage':
				$path = '/storage.json';
				break;
			case 'plugin-info':
				$path = '/storage/'.$extra.'.json';
				break;
			case 'enable_plugin':
				$path = '/storage/'.$extra.'/enable/true';
				break;
			case 'disable_plugin':
				$path = '/storage/'.$extra.'/enable/false';
				break;
			default:
				$path = '';
		}

		return $protocol . $this->hostname .':'. $this->port . $path;
	}

	/**
	 * Initiate GET Request to Drill server
	 *
	 * @param string $url Full URL Request to Drill Server
	 * @return array Associative array
	 */
	private function get_request(string $url): array {
		$ch = curl_init($url);
		curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
		curl_setopt($ch, CURLOPT_HEADER, 0);

		$response = curl_exec($ch);
		curl_close($ch);

		return json_decode($response, true);
	}

	/**
	 * Initiate POST Request to Drill server
	 *
	 * @param string $url Full URL Request to Drill Server
	 * @param array $postData Associative array of data
	 * @return array returns associative array
	 */
	private function post_request(string $url, array $postData): array {
		// Setup cURL
		$ch = curl_init($url);
		curl_setopt_array($ch, array(
				CURLOPT_POST => TRUE,
				CURLOPT_RETURNTRANSFER => TRUE,
				CURLOPT_HTTPHEADER => array('Content-Type: application/json'),
				CURLOPT_POSTFIELDS => json_encode($postData)
			)
		);
		$response = curl_exec($ch);
		curl_close($ch);

		return json_decode($response, true);
	}

	// endregion
}