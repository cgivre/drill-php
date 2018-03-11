# PHP Library to Connect to Apache Drill.

This library allows you to connect to and query Apache Drill programmatically.  It is loosely modeled after PHP's mysql interface, so if you are familiar with that, you already pretty much know how to use the Drill connector.

## Installing the Connector
The connector is on the Packageist (https://packagist.org/packages/thedataist/drill-connector) and can be installed by using composer as follows:
```
composer require thedataist/drill-connector
```

## Using the Connector
The first step is to connect to Drill.  The module uses Drill's RESTful interface, so it doesn't really make a "connection" in the same sense as MySQL.  

```php
  $drill = new DrillConnection( 'localhost', 8047 );
```

As mentioned earlier, this creates the object, but doesn't actually send anything to Drill. You can use the `is_active()` menthod to verify that your connection is active.
```php
  if( $drill->is_active() ) {
    print( "Connection Active" );
  } else {
    print( "Connection Inactive" );
  }
```

## Querying Drill
Now that you've connected to Drill, you can query drill in much a similar way as MySQL by calling the `query()` method. Once you've called the `query()` method, you can use one of the `fetch()` methods to retrieve the results, in a similar manner as MySQL.  Currently the Drill connector currently has:
* **`fetch_all()`**:  Returns all query results in an associative array.
* **`fetch_assoc()`**:  Returns a single query row as an associative array.
* **`fetch_object()`**:  Returns a single row as a PHP Object.

You might also find these functions useful:
* **`data_seek($n)`**: Returns the row at index `$n` and sets the current row to `$n`. 
* **`num_rows()`**: Returns the number of rows returned by the query.
* **`field_count()`**:  Returns the number of columns returned by the query.

Thus, if you want to execute a query in Drill, you can do so as follows:
```
$query_result = $drill->query( "SELECT * FROM cp.`employee.json` LIMIT 20" );
while( $row = $query_result->fetch_assoc() ) {
  print( "Field 1: {$row['field1']}\n" );
  print( "Field 2: {$row['field2']}\n" );
}
```
## Interacting with Drill
You can also use the connector to activate/deactivate Drill's storage as well as get information about Drill's plugins.

* **`disable_plugin( $plugin )`**  Disables the given plugin.  Returns true if successful, false if not.
* **`enable_plugin( $plugin )`**   Enables the given plugin.  Returns true if successful, false if not.
* **`get_all_storage_plugins()`**  Returns an array of all storage plugins.
* **`get_disabled_storage_plugins()`**  Returns an array of all disabled plugins.
* **`get_enabled_storage_plugins()`**  Returns an array of all enabled plugins.
* **`get_storage_plugins()`**  Returns an associative array of plugins and associated configuration options for all plugins.
* **`get_storage_plugin_info( $plugin )`**  Returns an associative array of configuration options for a given plugin. 

