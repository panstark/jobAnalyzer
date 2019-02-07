package com.sea.spark.utils

import java.util.UUID

import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.types._
import org.apache.spark.sql.{Row, types}


object DataConvertUtil {

  val struct = types.StructType(
      Array(
        StructField("jobUrl",StringType),//1
        StructField("jobName",StringType),//2
        StructField("jobAddr",StringType),//3
        StructField("jobDesc",StringType),//4
        StructField("salaryYearLow",StringType),//5
        StructField("salaryYearHigh",StringType),//6
        StructField("welfare",StringType),//7
        StructField("education",StringType),//8
        StructField("workYears",StringType),//9
        StructField("age",StringType),//10
        StructField("language",StringType),//11
        StructField("companyName",StringType),//12
        StructField("companyType",StringType),//13
        StructField("companyAddr",StringType),//14
        StructField("companySize",StringType),//15
        StructField("companyDesc",StringType),//16
        StructField("publishDate",StringType),//17
        StructField("crawlDate",StringType)//18
      )
    )


  def parseJobLiepin(jobLiepin:Row) ={

    try{
        val jobUrl = jobLiepin.getAs[String]("job_url")
        val jobName = jobLiepin.getAs[String]("job_name")
        val jobAddr = jobLiepin.getAs[String]("job_addr")
        val jobDesc = jobLiepin.getAs[String]("job_desc")
        val salaryYear = jobLiepin.getAs[String]("salary")
        var salaryYearLow = ""
        var salaryYearHigh = ""
        var salarys = new Array[String](2)
        if(salaryYear!=null){
          salarys = salaryYear .replace("\n","").replace("\r","")
            .replaceAll("\t","").replace("面议","00-00").
            replace("以下","-00").replaceAll("[\u4e00-\u9fa5]","").split("-")
          salaryYearLow=salarys(0)
          salaryYearHigh=salarys(1)
          if(StringUtils.isBlank(salaryYearLow)){
            salaryYearLow="0"
          }
          if(StringUtils.isBlank(salaryYearHigh)){
            salaryYearHigh="0"
          }
        }
        val welfare= jobLiepin.getAs[String]("welfare")
        val education = jobLiepin.getAs[String]("education")
        var workYears = jobLiepin.getAs[String]("work_years")
        if(workYears!=null) {
          workYears = workYears.replace("经验不限", "0").replace("年以上", "")
            .replaceAll("[\u4e00-\u9fa5]","")
          if(StringUtils.isBlank(workYears)){
            workYears="0"
          }
        }

        val age = jobLiepin.getAs[String]("age")
        val language = jobLiepin.getAs[String]("language")
        var companyName = jobLiepin.getAs[String]("company_name")
          if(companyName!=null){
            companyName=  companyName.replaceAll(" ","")
              .replace("\n","").replace("\t","").replace("\r","")
          }

        val companyType = jobLiepin.getAs[String]("company_type")
        var companyAddr = jobLiepin.getAs[String]("company_addr")
        if(companyAddr!=null){
          companyAddr = companyAddr.replace("公司地址：","").replace("公司地址","")
        }
        var companySize = jobLiepin.getAs[String]("company_size")
        if(companySize!=null){
          companySize =companySize.replace("公司规模：","").replace("公司规模","")
        }
        val companyDesc = jobLiepin.getAs[String]("company_desc")
        var publishDate = jobLiepin.getAs[String]("publish_date")
        if(publishDate!=null){
          publishDate = publishDate.replace("年","-").replace("月","-").replace("日","")
        }
        val crawlDate = jobLiepin.getAs[String]("crawl_date")

      Row(jobUrl,
        jobName,
        jobAddr,
        jobDesc,
        salaryYearLow,
        salaryYearHigh,
        welfare,
        education,
        workYears,
        age,
        language,
        companyName,
        companyType,
        companyAddr,
        companySize,
        companyDesc,
        publishDate,
        crawlDate)
    }catch{
      case e:Exception => Row(0)
    }
  }

  /*def jobByPublishDate(x: Row):Row= {

  }*/

}
