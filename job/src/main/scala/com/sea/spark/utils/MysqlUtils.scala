package com.sea.spark.utils

import java.sql.{Connection, DriverManager, PreparedStatement}

/**
  * mysql操作
  */
object MysqlUtils {

  /**
    * 拿到链接
    */
  def getConnection() = {
    DriverManager.getConnection("jdbc:mysql://localhost:3306/jeesite?user=root&password=Root.20182019")

  }

  /**
    * 释放数据库链接等资源
    * @param connection
    * @param pstmt
    */
  def release(connection:Connection,pstmt:PreparedStatement): Unit ={
    try{
      if(pstmt != null){
        pstmt.close()
      }
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      if(connection != null){
        connection.close()
      }
    }
  }

  def main(args: Array[String]): Unit = {
    print(getConnection())
  }

}
