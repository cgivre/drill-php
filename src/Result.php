<?
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
        protected $schema = [];

        function __construct($response, $query){
          $this->columns = $response['columns'];
          $this->rows = $response['rows'];
          $this->metadata = $response['metadata'];
          $this->query = $query;
          $this->row_pointer = 0;

          for( $i = 0; $i < count($this->columns); $i++) {
            $info = [];
            $info['column'] = $this->columns[$i];
            $info['data_type'] = self::clean_data_type_name($this->metadata[$i]);
            array_push($this->schema, $info);
          }
        }

      /**
       * This function cleans the data types and specifically removes precision information
       * from VARCHAR and DECIMAL data types which is not useful for UI work.
       * @param $dataType String The string data type which should be a Drill MinorType
       * @return String The datatype without precision information
       */
        static function clean_data_type_name($dataType) {
          $pattern = "/[a-zA-Z]+\(\d+(,\s*\d+)?\)/";
          if (preg_match($pattern, $dataType)) {
            $parts = explode('(', $dataType);
            $clean_data_type = $parts[0];
          } else {
            $clean_data_type = $dataType;
          }

          return $clean_data_type;
        }

        function data_seek( $n ){
          if( ! is_int( $n ) ){
            return false;
          } elseif( $n > count( $this->rows ) ){
            return false;
          } else{
            $this->row_pointer = $n;
            return true;
          }
        }

        function fetch_all(){
          return $this->rows;
        }

        function fetch_assoc(){
          if( $this->row_pointer >= count( $this->rows ) ){
            return false;
          } else {
            $result = $this->rows[ $this->row_pointer];
            $this->row_pointer++;
            return $result;
          }
        }

        function get_schema() {
          return $this->schema;
        }

        function fetch_object() {
          if( $this->row_pointer >= count( $this->rows ) ){
            return false;
          } else {
            $result = $this->rows[ $this->row_pointer];
            $result_object = new \stdClass();
            foreach ($result as $key => $value)
            {
              $result_object->$key = $value;
            }
            $this->row_pointer++;
            return $result_object;
          }
        }

        function field_count() {
          return count( $this->columns );
        }

        function more_results(){
          return $this->row_pointer < count( $this->rows );
        }

        function num_rows(){
          return count( $this->rows );
        }
    }

?>
