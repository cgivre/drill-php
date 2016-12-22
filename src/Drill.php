<?
    namespace Drill;
    use Drill\Result;
    include( "Result.php");

    /**
    * @package Drill
    * @author Charles Givre <cgivre@thedataist.com>
    */

    class Drill {
        protected $hostname;
        protected $port;
        protected $username = null;
        protected $password = null;
        protected $ssh = false;
        protected $error_message = null;

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

        function __destruct() {
            //Not sure if this is really needed...
        }

        function is_active(){
            try {
                $result = @get_headers( "http://$this->hostname:$this->port" );
            } catch (Exception $e) {
                return false;
            }

            if( isset( $result[1] ) ) {
                return true;
            } else {
                return false;
            }
        }

        function query( $query ){
            $url = $this->build_url("query");

            $postData = array(  "queryType" =>"SQL", "query" => $query );

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

        function error_message() {
            if( isset( $this->error_message ) ) {
                return $this->error_message;
            }
        }

        private function build_url($function){
            if( $this->ssh ){
                $protocol = "https://";
            } else {
                $protocol = "http://";
            }
            if( ! isset( $function ) ) {
                return "$protocol$this->hostname:$this->port";
            } elseif( $function == "query" ) {
                return "$protocol$this->hostname:$this->port/query.json";
            }
        }

    }

    $d = new Drill( 'localhost', 8047 );
    $result = $d->query( "SELECT * FROM dfs.test.`Tuition.csvh` LIMIT 10" );
    if( ! $result ) {
        print( $d->error_message );
    }
    else{
      $fieldcount = $result->field_count();
      print( "Fields: $fieldcount\n" );
    }

    while( $row = $result->fetch_object() )
    {
      print( "$row->Family\n" );
    }

?>
