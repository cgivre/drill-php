<?
    namespace thedataist\Drill;
    use thedataist\Drill\Result;


    /**
    * @package Drill
    * @author Charles Givre <cgivre@thedataist.com>
    */

    class DrillConnection {
        protected $hostname;
        protected $port;
        protected $username = null;
        protected $password = null;
        protected $ssh = false;
        protected $error_message = null;
        protected $columns;
        protected $rows;

        function __construct( $host, $arg_port, $username="", $password="", $ssh=false ){
            $this->hostname = $host;
            $this->port = $arg_port;
            $this->username = $username;
            $this->password = $password;
            $this->ssh = $ssh;
            $this->columns = null;
            $this->rows = null;
            $this->error_message = null;

        }

        function disable_plugin( $plugin ){
          $url = $this->build_url( "disable_plugin", $plugin );
          $result = $this->get_request( $url );
          if( isset($result['result']) && $result['result'] == "success" ) {
            return true;
          } else {
            return false;
          }
        }
        function enable_plugin( $plugin ){
          $url = $this->build_url("enable_plugin", $plugin);
          $result = $this->get_request( $url );
          if( isset($result['result']) && $result['result'] == "success" ) {
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
        function error_message() {
            if( isset( $this->error_message ) ) {
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
        function get_all_storage_plugins() {
          $plugin_info = $this->get_storage_plugins();
          $all_plugins = [];
          foreach ($plugin_info as $plugin) {
            array_push( $all_plugins, $plugin['name']);
          }

          return $all_plugins;

        }

        /**
        * This function returns an array of storage plugins which are enabled.
        *
        * @return array The list of enabled storage plugins, empty array if none
        */
        function get_enabled_storage_plugins() {
          $plugin_info = $this->get_storage_plugins();
          $enabled_plugins = [];
          foreach ($plugin_info as $plugin) {
            if( $plugin['config']['enabled'] == 1 ){
              array_push( $enabled_plugins, $plugin['name']);
            }
          }
          return $enabled_plugins;
        }


        /**
        * This function returns an associative array of storage plugins.  It will have all configuration options for the plugins.
        *
        * @return associative array The list storage plugins, empty array if none
        */
        function get_storage_plugins() {
          $url = $this->build_url("storage");
          $storage_info = $this->get_request( $url );
          return $storage_info;
        }

        /**
        * This function returns an array of configuration options for a given storage plugin.
        *
        * @param string $plugin The plain text name of the storage plugin.
        *
        * @return array Array containing all configuration options for the given plugin
        */
        function get_storage_plugin_info( $plugin ){
          $url = $this->build_url("plugin-info", $plugin);
          $storage_info = $this->get_request( $url );
          return $storage_info;
        }

        /**
        * This function returns true if the connection is active, false if not.
        *
        * @return boolean Returns true if the connection to Drill is active, false if not
        */
        function is_active(){
            try {
                $result = @get_headers( "http://$this->hostname:$this->port" );
            } catch (Exception $e) {
                $this->error_message = $e;
                return false;
            }

            if( isset( $result[1] ) ) {
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
        function query( $query ) {
            $url = $this->build_url("query");

            $postData = array( "queryType" =>"SQL", "query" =>  $query );

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
            $response = json_decode( $response, true );
            curl_close($ch);

            if( isset( $response['errorMessage'] ) ){
                $this->error_message = $response['errorMessage'];
                return false;
            } else {
                $r = new Result( $response, $query );
                return $r;
            }

        }

        private function build_url($function, $extra=""){
            if( $this->ssh ){
                $protocol = "https://";
            } else {
                $protocol = "http://";
            }
            if( ! isset( $function ) ) {
                return "$protocol{$this->hostname}:{$this->port}";
            } elseif( $function == "query" ) {
                return "$protocol$this->hostname:$this->port/query.json";
            } elseif( $function == "storage" ) {
                return "$protocol{$this->hostname}:{$this->port}/storage.json";
            } elseif( $function == "plugin-info" ) {
              return "$protocol{$this->hostname}:{$this->port}/storage/$extra.json";
            } elseif( $function == "enable_plugin" ){
              return "$protocol{$this->hostname}:{$this->port}/storage/$extra/enable/true";
            } elseif( $function == "disable_plugin" ){
              return "$protocol{$this->hostname}:{$this->port}/storage/$extra/enable/false";
            }
        }

        private function get_request( $url ){
          $ch = curl_init($url);
          curl_setopt($ch, CURLOPT_RETURNTRANSFER, true);
          curl_setopt($ch, CURLOPT_HEADER, 0);
          $response = curl_exec($ch);
          $response = json_decode( $response, true );
          curl_close($ch);
          return $response;
        }

    }

?>
