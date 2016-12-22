<?
    namespace Drill;

    /**
    * @package Drill
    * @author Charles Givre <cgivre@thedataist.com>
    */

    class Result {
        protected $columns;
        protected $rows;
        protected $query;
        protected $row_pointer;

        function __construct($response, $query){
          $this->columns = $response['columns'];
          $this->rows = $response['rows'];
          $this->query = $query;
          $this->row_pointer = 0;
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

        function num_rows(){
          return count( $this->rows);
        }
    }

?>
