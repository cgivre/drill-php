<?php
  namespace thedataist\Drill;
  use PHPUnit\Framework\TestCase;
  use thedataist\Drill\Result;
  use thedataist\Drill\DrillConnection;


  class DrillTest extends TestCase
  {
    protected $drill = null;

    public function testConnection() {
      $this->drill = new DrillConnection( 'localhost', 8047 );
      $active = $this->drill->is_active();
      $this->assertEquals( 1, $active );

    }
    public function testBadConnection() {
      $this->baddrill = new DrillConnection( 'localhost', 8048 );
      $active = $this->baddrill->is_active();
      $this->assertEquals( 0, $active );
    }

    public function testQuery() {
      $this->drill = new DrillConnection( 'localhost', 8047 );
      $result = $this->drill->query( "SELECT * FROM cp.`employee.json` LIMIT 5" );
      $this->assertEmpty( $this->drill->error_message());
      $fieldcount = $result->field_count();
      $this->assertEquals( $fieldcount, 16);
    }

    public function testPlugins() {
      $d = new DrillConnection( 'localhost', 8047 );
      $plugins =  $d->get_all_storage_plugins();
      $this->assertEquals( count( $plugins ), 7 );

      $enabledPlugins = $d->get_enabled_storage_plugins();
      $this->assertEquals( count($enabledPlugins),2 );

      $d->enable_plugin("hbase");
      $enabledPlugins = $d->get_enabled_storage_plugins();
      $this->assertEquals( count($enabledPlugins),3 );

      $d->disable_plugin("hbase");
      $enabledPlugins = $d->get_enabled_storage_plugins();
      $this->assertEquals( count($enabledPlugins),2 );

    }

  }
