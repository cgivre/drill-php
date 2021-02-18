<?php

namespace thedataist\Drill;


class DrillColumn
{
  protected $name;
  protected $dataType;

  function __construct($name, $dataType){
    $this->name = $name;
    $this->dataType = $dataType;
  }
}
?>