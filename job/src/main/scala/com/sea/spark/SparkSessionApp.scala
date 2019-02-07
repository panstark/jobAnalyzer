package com.sea.spark

import java.util.UUID

import com.sea.spark.dao.JobLiepinDao
import com.sea.spark.entity.JobLiepin
import org.apache.spark.sql.{DataFrame, SparkSession}

import scala.collection.mutable.ListBuffer

/**
  * sparkSession 使用
  */
object SparkSessionApp {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("sparkSessionApp").master("local[2]").getOrCreate()

    val liepinJob = spark.read.json("file:///Users/panstark/Documents/data/job/liepin_it_jobs.json")

    //liepinJob.printSchema()
    //liepinJob.show()

    //insertItJob(spark,liepinJob)
    liepinJob.rdd.take(3).foreach(println(_))

    spark.stop()
  }

  /**
    * 按照流量统计topN课程
    * @param spark
    * @param accessDF
    * @return
    */
  def insertItJob(spark: SparkSession, accessDF: DataFrame) = {
      var uuid = UUID.randomUUID();
    try{
      accessDF.foreachPartition(partionOfRecords =>{
        val list = new ListBuffer[JobLiepin]

        partionOfRecords.foreach(info=>{
          var jobUrlMd5 = info.getAs[String]("job_url_md5")
          if(jobUrlMd5==null){
            jobUrlMd5 = uuid.toString.replace("-","")
          }
          var jobUrl = info.getAs[String]("job_url")
          if(jobUrl!=null&&jobUrl.size>200){
            jobUrl = jobUrl.substring(0,200)
          }
          var jobName = info.getAs[String]("job_name")
          if(jobName!=null&&jobName.size>50){
            jobName = jobName.substring(0,49)
          }
          var jobAddr = info.getAs[String]("job_city")
          if(jobAddr!=null&&jobAddr.size>100){
            jobAddr=jobAddr.substring(0,100)
          }
          var jobDesc = info.getAs[String]("job_desc")
          if(jobDesc!=null&&jobDesc.size>2000){
            jobDesc = jobDesc.substring(0,2000)
          }
          var salaryYear = info.getAs[String]("sarary")
          var salaryYearLow = ""
          var salaryYearHigh = ""
          var salarys = new Array[String](2)
          if(salaryYear!=null){
            salarys = salaryYear.replace("面议","00-00").
              replace("万", "").replace("以下","-00").split("-")
            salaryYearLow=salarys(0)
            salaryYearHigh=salarys(1)
          }


          var welfare= info.getAs[String]("welfare")
          if(welfare!=null&&welfare.size>50){
            welfare = welfare.substring(0,50)
          }
          var education = info.getAs[String]("degree")
          if(education!=null&&education.size>50){
            education=education.substring(0,50)
          }
          var workYears = info.getAs[String]("work_years")
          if(workYears!=null){
            workYears = workYears.replace("年以上","").replace("年","")
          }
          val age = info.getAs[String]("age")
          var language = info.getAs[String]("language")
          if(language!=null&&language.size>30){
            language =language.substring(0,30)
          }
          var companyName = info.getAs[String]("company_name")
          if(companyName!=null&&companyName.size>30){
            companyName=companyName.substring(0,30)
          }
          var companyType = info.getAs[String]("company_type")
          if(companyType!=null&&companyType.size>50){
            companyType=companyType.substring(0,50)
          }
          var companyAddr = info.getAs[String]("company_addr")
          if(companyAddr!=null){
            companyAddr = companyAddr.replace("公司地址：","")
            if(companyAddr.size>100){
              companyAddr=companyAddr.substring(0,100)
            }
          }
          var companySize = info.getAs[String]("company_size")
          if(companySize!=null){
            companySize =companySize.replace("公司规模：","").replace("公司规模","")
            if(companySize.size>30){
              companySize = companySize.substring(0,30)
            }
          }
          var companyDesc = info.getAs[String]("company_desc")
          if (companyDesc!=null&&companyDesc.length > 500) {
            companyDesc = companyDesc.substring(0,475)+"..."
          }
          var publishDate = info.getAs[String]("publish_time")
          if(publishDate!=null){
            publishDate = publishDate.replace("年","-").replace("月","-").replace("日","")
          }
          val crawlDate = info.getAs[String]("crawl_time")

          list.append(JobLiepin(jobUrlMd5:String,jobUrl: String,
            jobName:String,jobAddr:String,jobDesc:String,salaryYearLow: String,salaryYearHigh:String,welfare:String,
            education:String,workYears:String,age:String,language:String,
            companyName:String,companyType:String,companyAddr:String,companySize:String,companyDesc:String,
            publishDate:String,crawlDate:String))
        })
        JobLiepinDao.insertItJob(list)

      })
    }catch{
      case e:Exception => e.printStackTrace()
    }
  }
}
