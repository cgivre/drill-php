<?
namespace thedataist\Drill;

use thedataist\Drill\Result;


/**
 * @package Drill
 * @author Charles Givre <cgivre@thedataist.com>
 */
class DrillConnection
{
  protected $hostname;
  protected $port;
  protected $username = null;
  protected $password = null;
  protected $ssh = false;
  protected $error_message = null;
  protected $columns;
  protected $rows;
  protected $cached_plugins = null;
  protected $default_schema = null;
  protected $row_limit;
  protected $cached_enabled_plugins = null;

  function __construct($host, $arg_port, $username = "", $password = "", $ssh = false, $row_limit = 10000)
  {
    $this->hostname = $host;
    $this->port = $arg_port;
    $this->username = $username;
    $this->password = $password;
    $this->ssh = $ssh;
    $this->columns = null;
    $this->rows = null;
    $this->error_message = null;
    $this->row_limit = $row_limit;
    $this->cached_enabled_plugins = $this->get_enabled_storage_plugins();
  }

  function disable_plugin($plugin)
  {
    $url = $this->build_url("disable_plugin", $plugin);
    $result = $this->get_request($url);
    if (isset($result['result']) && $result['result'] == "success") {
      return true;
    } else {
      return false;
    }
  }

  function enable_plugin($plugin)
  {
    $url = $this->build_url("enable_plugin", $plugin);
    $result = $this->get_request($url);
    if (isset($result['result']) && $result['result'] == "success") {
      return true;
    } else {
      return false;
    }
  }

  /**
   * This function returns the error message from the most recent query, an empty string if undefined.
   *
   * @return string The error message from the most recent query
   */
  function error_message()
  {
    if (isset($this->error_message)) {
      return $this->error_message;
    } else {
      return "";
    }
  }

  /**
   * This function returns an array of all storage plugins.
   *
   * @return array The list of all storage plugins, empty array if none
   */
  function get_all_storage_plugins()
  {
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
   * This function returns an array of storage plugins which are disabled.
   *
   * @return array The list of disabled storage plugins, empty array if none
   */
  function get_disabled_storage_plugins()
  {
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
   * This function returns an array of storage plugins which are enabled.
   *
   * @return array The list of enabled storage plugins, empty array if none
   */
  function get_enabled_storage_plugins()
  {
    if (!$this->is_active()) {
      return null;
    }
    $plugin_info = $this->get_storage_plugins();
    $enabled_plugins = [];
    foreach ($plugin_info as $plugin) {
      if ($plugin['config']['enabled'] == 1) {
        array_push($enabled_plugins, $plugin['name']);
      }
    }
    $this->cached_enabled_plugins = $enabled_plugins;
    return $enabled_plugins;
  }

  /**
   * Returns the cached list of enabled plugins.  Theoretically you can reduce API calls with this method.
   * @return associative array The list storage plugins, empty array if none
   */
  function get_cached_enabled_plugins()
  {
    if ($this->cached_enabled_plugins == null) {
      $this->cached_enabled_plugins = $this->get_enabled_storage_plugins();
    }
    return $this->cached_enabled_plugins;
  }

  /**
   * Returns the cached list of plugins.  Theoretically you can reduce API calls with this method.
   * @return associative array The list storage plugins, empty array if none
   */
  function get_cached_plugins()
  {
    if ($this->cached_plugins == null) {
      $this->cached_plugins = $this->get_storage_plugins();
    }
    return $this->cached_plugins;
  }

  /**
   * This function returns an associative array of storage plugins.  It will have all configuration options for the plugins.
   *
   * @return associative array The list storage plugins, empty array if none
   */
  function get_storage_plugins()
  {
    $url = $this->build_url("storage");
    $storage_info = $this->get_request($url);
    return $storage_info;
  }

  /**
   * This function returns an array of configuration options for a given storage plugin.
   *
   * @param string $plugin The plain text name of the storage plugin.
   *
   * @return array Array containing all configuration options for the given plugin
   */
  function get_storage_plugin_info($plugin)
  {
    $url = $this->build_url("plugin-info", $plugin);
    $storage_info = $this->get_request($url);
    return $storage_info;
  }

  /**
   * This function returns true if the connection is active, false if not.
   *
   * @return boolean Returns true if the connection to Drill is active, false if not
   */
  function is_active()
  {
    try {
      $result = @get_headers("http://$this->hostname:$this->port");
    } catch (Exception $e) {
      $this->error_message = $e;
      return false;
    }

    if (isset($result[1])) {
      return true;
    } else {
      return false;
    }
  }

  /**
   * This function executes a Drill query.  The function returns true if the query executed successfully, false if not.
   *
   * @return boolean Returns true if the query executed successfully, false if not.
   */
  function query($query)
  {
    $url = $this->build_url("query");

    $postData = array("queryType" => "SQL", "query" => $query, "autoLimit" => $this->row_limit);

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
    $response = json_decode($response, true);
    curl_close($ch);

    if (isset($response['errorMessage'])) {
      $this->error_message = $response['errorMessage'];
      return false;
    } else {
      $r = new Result($response, $query);
      return $r;
    }

  }

  private function build_url($function, $extra = "")
  {
    if ($this->ssh) {
      $protocol = "https://";
    } else {
      $protocol = "http://";
    }
    if (!isset($function)) {
      return "$protocol{$this->hostname}:{$this->port}";
    } elseif ($function == "query") {
      return "$protocol$this->hostname:$this->port/query.json";
    } elseif ($function == "storage") {
      return "$protocol{$this->hostname}:{$this->port}/storage.json";
    } elseif ($function == "plugin-info") {
      return "$protocol{$this->hostname}:{$this->port}/storage/$extra.json";
    } elseif ($function == "enable_plugin") {
      return "$protocol{$this->hostname}:{$this->port}/storage/$extra/enable/true";
    } elseif ($function == "disable_plugin") {
      return "$protocol{$this->hostname}:{$this->port}/storage/$extra/enable/false";
    }
  }

  private function get_request($url)
  {
    $ch = curl_init($url);
    curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
    curl_setopt($ch, CURLOPT_HEADER, 0);
    $response = curl_exec($ch);
    $response = json_decode($response, true);
    curl_close($ch);
    return $response;
  }

  /**
   * @return array|null A list of enabled schemata
   */
  function get_schema_names() {
    if (! $this->is_active()) {
      return null;
    }

    $query = "SHOW DATABASES";
    $schemata = [];
    $raw_results = $this->query($query)->fetch_all();
    if (!$raw_results) {
      print("Error retrieving schema names");
      return null;
    }

    foreach ($raw_results as $result) {
      $schema = $result['SCHEMA_NAME'];
      if ($schema != "cp.default" and
        $schema != 'INFORMATION_SCHEMA' and
        $schema != 'information_schema' and
        $schema != "dfs.default" and
        $schema != "sys") {
        array_push($schemata, $schema);
      }
    }

    return $schemata;
  }

  function get_view_names($schema) {
    if (! $this->is_active()) {
      return null;
    }

    $view_names = [];
    $sql = "SELECT `TABLE_NAME` FROM INFORMATION_SCHEMA.views WHERE table_schema='$schema'";
    $results = $this->query($sql)->fetch_all();

    foreach ($results as $result) {
      $schema = $result['TABLE_NAME'];
      array_push($view_names, $schema);
    }
    return $view_names;
  }

  function format_drill_table($schema, $is_file)
  {
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

  function get_plugin_type($plugin) {
    if ($plugin == null) {
      return null;
    } else if (!$this->is_active()) {
      return null;
    }

    // Remove back ticks
    $plugin = str_replace("`", "", $plugin);

    $query = "SELECT SCHEMA_NAME, TYPE FROM INFORMATION_SCHEMA.`SCHEMATA` WHERE SCHEMA_NAME LIKE '%$plugin%' LIMIT 1";
    // Should only be one row
    $info = $this->query($query)->fetch_assoc();
    if ($info == null) {
      return null;
    }
    return strtolower($info['TYPE']);
  }

  /** This function returns a list of Drill tables which exist in a Drill schema.  For a
   * file system, this is essentially a list of files in a given workspace.  For a relational
   * database, this corresponds to the list of tables in the database.
   * @param $schema String The input schema
   * @return array List of table names
   */
  function get_table_names($schema) {
    $clean_schema = str_replace('`', '', $schema);
    $plugin_type = $this->get_plugin_type($clean_schema);
    $table_names = [];

    if ($plugin_type == "file") {
      $sql = "SELECT FILE_NAME FROM information_schema.`files` WHERE SCHEMA_NAME = '$clean_schema' AND IS_FILE = true";
      $tables = $this->query($sql)->fetch_all();

      foreach ($tables as $table) {
        if (strpos($table['FILE_NAME'], "view.drill")) {
          // Skip views.  Use the get_view_names() method instead
          continue;
        } else {
          array_push($table_names, $table['FILE_NAME']);
        }
      }
    } else {
      $sql = "SELECT `TABLE_NAME` FROM INFORMATION_SCHEMA.`TABLES` WHERE `TABLE_SCHEMA` = '$clean_schema'";
      $tables = $this->query($sql)->fetch_all();

      foreach ($tables as $table) {
        if (strpos($table['TABLE_NAME'], "view.drill")) {
          $table_name = str_replace("view.drill", "", $table['TABLE_NAME']);
        } else {
          $table_name = $table['TABLE_NAME'];
        }
        array_push($table_names, $table_name);
      }
    }
    return $table_names;
  }

  /** This function returns the columns present in a given data source.
   * Drill schema discovery behaves differently for the different plugin types.  If the data is a
   * file, API, MongoDB or Splunk, we have to execute a `SELECT *` query with a LIMIT 1 to identify the columns
   * and data types.
   *
   * If the data is a database, we can use the DESCRIBE TABLE command to access schema information.
   *
   * @param $table_name String The table or file name
   * @param $schema String The schema or plugin name
   */
  function get_columns($table_name, $schema) {
    $result = [];
    $plugin_type = $this->get_plugin_type($schema);

    // Since MongoDB uses the ** notation, bypass that and query the data directly
    // TODO Add API functionality here as well
    if ($plugin_type == "file" or $plugin_type == "mongo" or $plugin_type == "splunk") {

      $views  = $this->get_view_names($schema);

      $file_name = "$schema.$table_name";

      if ($plugin_type == "mongo") {
        $mongo_quoted_file_name = $this->format_drill_table($file_name, false);
        $sql = "SELECT ** FROM $mongo_quoted_file_name LIMIT 1";
      } else if (in_array($table_name, $views)) {
        $view_name = "`$schema`.`$table_name`";
        $sql = "SELECT * FROM $view_name LIMIT 1";
      } else if ($plugin_type == "splunk") {
        // Case for Splunk
        $splunk_quoted_file_name = $this->format_drill_table($file_name, false);
        $sql = "SELECT * FROM $splunk_quoted_file_name LIMIT 1";
      } else {
        $quoted_file_name = $this->format_drill_table($file_name, true);
        $sql = "SELECT * FROM $quoted_file_name LIMIT 1";
      }
      $column_metadata = $this->query($sql)->get_schema();
      return $column_metadata;

    } else if (str_contains($table_name, "SELECT")) {
      $sql = "SELECT * FROM $table_name LIMIT 1";
    } else {
      /*
       * Case for everything else.
       */
      $quoted_schema = $this->format_drill_table("$schema.$table_name", false);
      $sql = "DESCRIBE $quoted_schema";
    }

    $column_metadata = $this->query($sql)->get_schema();
    return $column_metadata;
  }
}
?>
