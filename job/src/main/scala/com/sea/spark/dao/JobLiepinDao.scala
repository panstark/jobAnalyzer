package com.sea.spark.dao

import java.sql.{Connection, PreparedStatement}

import com.sea.spark.entity.{JobCompany, JobDayCount, JobLiepin}
import com.sea.spark.utils.MysqlUtils

import scala.collection.mutable.ListBuffer

object JobLiepinDao {

  def insertItJob(list: ListBuffer[JobLiepin]): Unit ={
      var connection:Connection = null
      var pstmt:PreparedStatement = null

      try{
        connection = MysqlUtils.getConnection()
        connection.setAutoCommit(false)
        val sql = "insert into job_liepin " +
          "(id,job_url,job_name,job_addr,job_desc,salary_year_low,salary_year_high,welfare,education," +
          "work_years,language,company_name,company_type,company_addr,company_size,company_desc," +
          "publish_date,crawl_date,duplicate_num) " +
          "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?) on duplicate key update duplicate_num=duplicate_num+1";

        pstmt = connection.prepareStatement(sql)
        for(ele <- list){
          pstmt.setString(1,ele.jobUrlMd5)
          pstmt.setString(2,ele.jobUrl)
          pstmt.setString(3,ele.jobName)
          pstmt.setString(4,ele.jobAddr)
          pstmt.setString(5,ele.jobDesc)
          pstmt.setString(6,ele.salaryYearLow)
          pstmt.setString(7,ele.salaryYearHigh)
          pstmt.setString(8,ele.welfare)
          pstmt.setString(9,ele.education)
          pstmt.setString(10,ele.workYears)
          pstmt.setString(11,ele.language)
          pstmt.setString(12,ele.companyName)
          pstmt.setString(13,ele.companyType)
          pstmt.setString(14,ele.companyAddr)
          pstmt.setString(15,ele.companySize)
          pstmt.setString(16,ele.companyDesc)
          pstmt.setString(17,ele.publishDate)
          pstmt.setString(18,ele.crawlDate)
          pstmt.setInt(19,1)
          pstmt.addBatch()

        }
        pstmt.executeBatch()
        connection.commit()
      }catch{
        case e:Exception => e.printStackTrace()
      }finally {
        MysqlUtils.release(connection,pstmt)
      }
    }


  def insertJobDayCount(list: ListBuffer[JobDayCount]): Unit ={
    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connection = MysqlUtils.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into job_day_count " +
        "(job_name,job_addr,salary_year_avg_low,salary_year_avg_high,job_num," +
        "work_years_avg,publish_date,crawl_date,source,create_by,update_by) " +
        "values (?,?,?,?,?,?,?,?,?,?,?)"


      pstmt = connection.prepareStatement(sql)
      for(ele <- list){
        pstmt.setString(1,ele.jobName)
        pstmt.setString(2,ele.jobAddr)
        pstmt.setDouble(3,ele.salaryYearAvgLow)
        pstmt.setDouble(4,ele.salaryYearAvgHigh)
        pstmt.setLong(5,ele.jobNum)
        pstmt.setDouble(6,ele.workYearsAvg)
        pstmt.setString(7,ele.publishDate)
        pstmt.setString(8,ele.crawlDate)
        pstmt.setString(9,"lie")
        pstmt.setString(10,"sys")
        pstmt.setString(11,"sys")

        pstmt.addBatch()

      }
      pstmt.executeBatch()
      connection.commit()
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      MysqlUtils.release(connection,pstmt)
    }
  }

  def insertJobCompany(list: ListBuffer[JobCompany]): Unit = {
    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connection = MysqlUtils.getConnection()
      connection.setAutoCommit(false)
      val sql = "insert into job_company " +
        "(company_name,job_name,job_addr,salary_year_avg_low,salary_year_avg_high,job_num," +
        "work_years_avg,publish_date,crawl_date,source,create_by,update_by) " +
        "values (?,?,?,?,?,?,?,?,?,?,?,?)"


      pstmt = connection.prepareStatement(sql)
      for(ele <- list){
        pstmt.setString(1,ele.companyName)
        pstmt.setString(2,ele.jobName)
        pstmt.setString(3,ele.jobAddr)
        pstmt.setDouble(4,ele.salaryYearAvgLow)
        pstmt.setDouble(5,ele.salaryYearAvgHigh)
        pstmt.setLong(6,ele.jobNum)
        pstmt.setDouble(7,ele.workYearsAvg)
        pstmt.setString(8,ele.publishDate)
        pstmt.setString(9,ele.crawlDate)
        pstmt.setString(10,"lie")
        pstmt.setString(11,"sys")
        pstmt.setString(12,"sys")

        pstmt.addBatch()

      }
      pstmt.executeBatch()
      connection.commit()
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      MysqlUtils.release(connection,pstmt)
    }
  }


  def insertTop30CompanyJob(list: ListBuffer[JobLiepin]): Unit = {
    var connection:Connection = null
    var pstmt:PreparedStatement = null

    try{
      connection = MysqlUtils.getConnection()
      connection.setAutoCommit(false)



      val sql = "insert into job_top_detail " +
        "(job_name,job_url,job_addr,job_desc,salary_year_low,salary_year_high," +
        "welfare,education,work_years,age,language,company_name,company_type,company_addr,company_size,company_desc," +
        "publish_date,crawl_date,source,create_by,update_by) " +
        "values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"


      pstmt = connection.prepareStatement(sql)
      for(ele <- list){
        pstmt.setString(1,ele.jobName)
        pstmt.setString(2,ele.jobUrl)
        pstmt.setString(3,ele.jobAddr)
        pstmt.setString(4,ele.jobDesc)
        pstmt.setString(5,ele.salaryYearLow)
        pstmt.setString(6,ele.salaryYearHigh)
        pstmt.setString(7,ele.welfare)
        pstmt.setString(8,ele.education)
        pstmt.setString(9,ele.workYears)
        pstmt.setString(10,ele.age)
        pstmt.setString(11,ele.language)
        pstmt.setString(12,ele.companyName)
        pstmt.setString(13,ele.companyType)
        pstmt.setString(14,ele.companyAddr)
        pstmt.setString(15,ele.companySize)
        pstmt.setString(16,ele.companyDesc)
        pstmt.setString(17,ele.publishDate)
        pstmt.setString(18,ele.crawlDate)
        pstmt.setString(19,"lie")
        pstmt.setString(20,"sys")
        pstmt.setString(21,"sys")
        pstmt.addBatch()
      }
      pstmt.executeBatch()
      connection.commit()
    }catch{
      case e:Exception => e.printStackTrace()
    }finally {
      MysqlUtils.release(connection,pstmt)
    }
  }



}
