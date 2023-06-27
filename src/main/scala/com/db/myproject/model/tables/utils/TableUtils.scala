package com.db.myproject.model.tables.utils

trait TableUtils[A, B] extends Serializable {

  def convertTimestampsForBQ(a: A): B

  def getTimeInsideMap(map: Option[Map[String,String]]): String = {
    if(map.isEmpty) "" else map.get("string")
  }

  def getLongInsideMap(map: Option[Map[String,Long]]): Long = {
    if(map.isEmpty) 0 else map.get("long")
  }

}
