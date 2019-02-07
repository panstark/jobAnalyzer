package com.sea.spark

import com.sea.spark.dao.JobLiepinDao
import com.sea.spark.entity.{JobCompany, JobDayCount, JobLiepin}
import com.sea.spark.utils.{DataConvertUtil, DateUtils}
import org.apache.spark.sql._

import scala.collection.mutable.ListBuffer


object DataClean {


  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("sparkSessionApp").master("local[2]").getOrCreate()


    val liepinJobs = spark.read.json("file:///Users/panstark/Documents/data/job/liepin_2019-02-06.json")
    val dateStr = "2019-02-06"

    //数据清洗并保存
    //RDD==>DF
    val jobLiepinDF = spark.createDataFrame(liepinJobs.rdd.map(x=>DataConvertUtil.parseJobLiepin(x)),DataConvertUtil.struct)
      //.write.save("file:///Users/panstark/Documents/data/job/clean/liepin3")

    jobLiepinDF.createTempView("jobLiepin")

    jobByPublishDate(spark,jobLiepinDF,dateStr)

    jobCompany(spark,jobLiepinDF,dateStr)

    insertJobTopDetail(spark,jobLiepinDF)

    spark.stop()
  }



  def jobByPublishDate(spark: SparkSession, jobLiepinDF: DataFrame,dateStr:String) = {

    val jobByPublishDate =  spark.sql("select " +
      "Round(avg(salaryYearLow),4) AS salaryYearAvgLow," +
      "Round(avg(salaryYearHigh),4) AS salaryYearAvgHigh," +
      "count(*) AS jobNum," +
      "Round(avg(workYears),4) AS workYearsAvg," +
      "publishDate" +
      " from jobLiepin " +
      " where (jobName like '%大数据%' or jobName like '%hadoop%' or jobName like '%spark%')" +
      " and jobAddr like '%北京%' and workYears !='' and workYears <=3 and length(salaryYearHigh)<5 and salaryYearLow>0" +
      " group by publishDate" +
      " order by publishDate desc")
    /**
      * 将统计结果写入到mysql中
      */
    try{
      jobByPublishDate.foreachPartition(partionOfRecords =>{
        val list = new ListBuffer[JobDayCount]

        partionOfRecords.foreach(info=>{

          val salaryYearAvgLow = info.getAs[Double]("salaryYearAvgLow")
          val salaryYearAvgHigh = info.getAs[Double]("salaryYearAvgHigh")
          val jobNum = info.getAs[Long]("jobNum")
          val workYearsAvg = info.getAs[Double]("workYearsAvg")
          val publishDate = info.getAs[String]("publishDate")

          list.append(JobDayCount("bigData","BeiJing",salaryYearAvgLow, salaryYearAvgHigh,jobNum,workYearsAvg,publishDate,dateStr))
        })

        JobLiepinDao.insertJobDayCount(list)
      })
    }catch{
      case e:Exception => e.printStackTrace()
    }
  }

  /**
    * 按公司与发布日期进行分组
    * @param spark
    * @param jobLiepinDF
    */
  def jobCompany(spark: SparkSession, jobLiepinDF: DataFrame,dateStr:String): DataFrame ={

    val jobByPublishDate =  spark.sql("select " +
      "companyName," +
      "Round(avg(salaryYearLow),4) AS salaryYearAvgLow," +
      "Round(avg(salaryYearHigh),4) AS salaryYearAvgHigh," +
      "count(1) AS jobNum," +
      "Round(avg(workYears),4) AS workYearsAvg," +
      "publishDate" +
      " from jobLiepin " +
      " where (jobName like '%大数据%' or jobName like '%hadoop%' or jobName like '%spark%')" +
      " and jobAddr like '%北京%' and workYears !='' and workYears <=3 and length(salaryYearHigh)<5 and salaryYearLow>0" +
      " group by companyName,publishDate " +
      " order by publishDate desc")

    /**
      * 将统计结果写入到mysql中
      */
    try{
      jobByPublishDate.foreachPartition(partionOfRecords =>{
        val list = new ListBuffer[JobCompany]

        partionOfRecords.foreach(info=>{
          val companyName = info.getAs[String]("companyName")
          val salaryYearAvgLow = info.getAs[Double]("salaryYearAvgLow")
          val salaryYearAvgHigh = info.getAs[Double]("salaryYearAvgHigh")
          val jobNum = info.getAs[Long]("jobNum")
          val workYearsAvg = info.getAs[Double]("workYearsAvg")
          val publishDate = info.getAs[String]("publishDate")

          list.append(JobCompany(companyName,"bigData","BeiJin",salaryYearAvgLow, salaryYearAvgHigh,jobNum,workYearsAvg,publishDate,dateStr))
        })

        JobLiepinDao.insertJobCompany(list)
      })
    }catch{
      case e:Exception => e.printStackTrace()
    }

    jobByPublishDate.select("companyName")
  }

  /**
    * 将排名前三十的公司发布的工作插入到数据库
    */
  def insertJobTopDetail(spark: SparkSession, jobLiepinDF: DataFrame): Unit ={

    //找到工资高的前三十个公司的工作
     val top30CompanyJob =spark.sql("select * from jobLiepin " +
       "where (jobName like '%大数据%' or jobName like '%hadoop%' or jobName like '%spark%') and jobAddr like '%北京%'" +
       " and companyName in (" +
       " select companyName from (select " +
       " companyName," +
       "(Round(avg(salaryYearLow),4)+Round(avg(salaryYearHigh),4))/2*count(1) AS salaryTotal" +
       " from jobLiepin " +
       " where (jobName like '%大数据%' or jobName like '%hadoop%' or jobName like '%spark%')" +
       " and jobAddr like '%北京%' and workYears !='' and workYears <=3 and length(salaryYearHigh)<5 and salaryYearLow>0" +
       " group by companyName,publishDate " +
       " order by salaryTotal desc" +
       " limit 30))")

   /// val companyNameList = List[String]()
    //查找三十个公司对应的职位
 // val top30CompanyJob = jobLiepinDF.filter(jobLiepinDF.col("companyName").isin("滴滴出行","汽车之家")).show(50)


    /**
      * 将统计结果写入到mysql中
      */
    try{
      top30CompanyJob.foreachPartition(partionOfRecords =>{
        val list = new ListBuffer[JobLiepin]
        partionOfRecords.foreach(info=>{
          val jobUrl = info.getAs[String]("jobUrl")
          val jobName = info.getAs[String]("jobName")
          val jobAddr = info.getAs[String]("jobAddr")
          val jobDesc = info.getAs[String]("jobDesc")
          val salaryYearLow = info.getAs[String]("salaryYearLow")
          val salaryYearHigh = info.getAs[String]("salaryYearHigh")
          val education = info.getAs[String]("education")
          val welfare= info.getAs[String]("welfare")

          val workYears = info.getAs[String]("workYears")
          val age = info.getAs[String]("age")
          val language = info.getAs[String]("language")
          val companyName = info.getAs[String]("companyName")
          val companyType = info.getAs[String]("companyType")
          val companyAddr = info.getAs[String]("companyAddr")
          val companySize = info.getAs[String]("companySize")
          val companyDesc = info.getAs[String]("companyDesc")
          val publishDate = info.getAs[String]("publishDate")
          val crawlDate = info.getAs[String]("crawlDate")

          list.append(JobLiepin("111":String,jobUrl: String,
            jobName:String,jobAddr:String,jobDesc:String,salaryYearLow: String,salaryYearHigh:String,welfare:String,
            education:String,workYears:String,age:String,language:String,
            companyName:String,companyType:String,companyAddr:String,companySize:String,companyDesc:String,
            publishDate:String,crawlDate:String))})

        JobLiepinDao.insertTop30CompanyJob(list)
      })
    }catch{
      case e:Exception => e.printStackTrace()
    }


  }

case class Company(companyName:String)


}
