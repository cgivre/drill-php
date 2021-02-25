<?php

namespace thedataist\Drill;

use PHPUnit\Framework\TestCase;

use thedataist\Drill\DrillConnection;
use thedataist\Drill\Result;


class DrillTest extends TestCase {
	
	protected $host = 'localhost';
	protected $port = 8047;
	protected $username = '';
	protected $password = '';
	protected $ssl = true;
	protected $row_limit = 10000;
	
	protected $drill = null;

	public function testConnection() {
		$this->drill = new DrillConnection($this->host, $this->port, $this->username, $this->password, $this->ssl, $this->row_limit);
		$active = $this->drill->is_active();
		$this->assertEquals(true, $active);

	}

	public function testBadConnection() {
		$this->baddrill = new DrillConnection($this->host, 8048);
		$active = $this->baddrill->is_active();
		$this->assertEquals(0, $active);
	}

	public function testQuery() {
		$this->drill = new DrillConnection($this->host, $this->port, $this->username, $this->password, $this->ssl, $this->row_limit);
		$result = $this->drill->query("SELECT * FROM cp.`employee.json` LIMIT 5");
		$this->assertEmpty($this->drill->error_message());
		$fieldcount = $result->field_count();
		$this->assertEquals(16, $fieldcount);
	}

	public function testPlugins() {
		$d = new DrillConnection($this->host, $this->port, $this->username, $this->password, $this->ssl, $this->row_limit);
		$plugins = $d->get_all_storage_plugins();
		$this->assertEquals(15, count($plugins));

		$enabledPlugins = $d->get_enabled_storage_plugins();
		$this->assertEquals(6, count($enabledPlugins));
	}

	public function testFormatTable() {
		$d = new DrillConnection($this->host, $this->port, $this->username, $this->password, $this->ssl, $this->row_limit);
		$file_with_workspace = $d->format_drill_table("dfs.test.data.csv", true);
		$this->assertEquals("dfs.`test`.`data.csv`", $file_with_workspace);

		$file_without_workspace = $d->format_drill_table("dfs.test.csv", true);
		$this->assertEquals("dfs.`test.csv`", $file_without_workspace);

		$file_with_workspace_and_backticks = $d->format_drill_table("`dfs`.`test`.`data.csv`", true);
		$this->assertEquals("dfs.`test`.`data.csv`", $file_with_workspace_and_backticks);

		$db_2_part = $d->format_drill_table("mysql.sales", false);
		$this->assertEquals("`mysql`.`sales`", $db_2_part);

		$db_3_part = $d->format_drill_table("mysql.sales.customers", false);
		$this->assertEquals("`mysql`.`sales`.`customers`", $db_3_part);
	}

	public function testSchemaNames() {
		$d = new DrillConnection($this->host, $this->port, $this->username, $this->password, $this->ssl, $this->row_limit);
		print_r($d->get_schema_names());
		$this->assertTrue(true);
	}

	public function testGetPluginType() {
		$d = new DrillConnection($this->host, $this->port, $this->username, $this->password, $this->ssl, $this->row_limit);
		$plugin_type = $d->get_plugin_type("dfs");
		$this->assertEquals("file", $plugin_type);
	}

	public function testGetTableNames() {
		$d = new DrillConnection($this->host, $this->port, $this->username, $this->password, $this->ssl, $this->row_limit);
		print_r($d->get_table_names('dfs', 'test'));
		$this->assertTrue(true);
	}

	public function testGetColumns() {
		$d = new DrillConnection($this->host, $this->port, $this->username, $this->password, $this->ssl, $this->row_limit);
		$x = $d->get_columns("Dummy Customers-1.xlsx", "dfs.test");
		print_r($x);
		$this->assertTrue(true);
	}

	public function testCleanDataTypeName() {
		$this->assertEquals("DECIMAL", Result::clean_data_type_name("DECIMAL(3, 4)"));
		$this->assertEquals("FLOAT8", Result::clean_data_type_name("FLOAT8"));
		$this->assertEquals("CHAR", Result::clean_data_type_name("CHAR(30)"));
	}
}
