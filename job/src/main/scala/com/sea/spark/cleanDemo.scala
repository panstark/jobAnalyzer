package com.sea.spark

import com.sea.spark.DataClean.insertJobTopDetail
import com.sea.spark.utils.DataConvertUtil
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object cleanDemo {

  def main(args: Array[String]): Unit = {

    val spark = SparkSession.builder().appName("sparkSessionApp").master("local[2]").getOrCreate()


    val liepinJobs = spark.read.json("file:///Users/panstark/Documents/data/job/liepin_2019-01-30.json")

    //数据清洗并保存
    //RDD==>DF
    val jobLiepinDF = spark.createDataFrame(liepinJobs.rdd.map(x=>DataConvertUtil.parseJobLiepin(x)),DataConvertUtil.struct)


    jobLiepinDF.createTempView("jobLiepin")

    insertJobTopDetail(spark,jobLiepinDF)

    spark.stop()
  }

  /**
    * 将排名前三十的公司发布的工作插入到数据库
    */
  def insertJobTopDetail(spark: SparkSession, jobLiepinDF: DataFrame): Unit ={

    //找到工资高的前三十个公司名称
    /**
      * 问题1、这个例如生成一个list<String> ，下面isin要用
      */
    val companyNameRowList = spark.sql("select " +
      "companyName," +
      "(Round(avg(salaryYearLow),4)+Round(avg(salaryYearHigh),4))/2*count(1) AS salaryTotal" +
      " from jobLiepin " +
      " where (jobName like '%大数据%' or jobName like '%hadoop%' or jobName like '%spark%')" +
      " and jobAddr like '%北京%' and workYears !='' and workYears <=3 and length(salaryYearHigh)<5 and salaryYearLow>0" +
      " group by companyName,publishDate " +
      " order by salaryTotal desc" +
      " limit 30").select("companyName")
    Row


    //查找三十个公司对应的职位
    /**
      * 这一步希望查找companyName为companyNameRowList中的所有job，但是这样写不生效
      * 不用isin的话，有没有其他方法可以把sql中in('','','')表示出来
      *
      */
    //val top30CompanyJob = jobLiepinDF.filter(jobLiepinDF.col("companyName").isin())

  }

}
