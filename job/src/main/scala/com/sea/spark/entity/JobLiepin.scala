package com.sea.spark.entity


case class JobLiepin(jobUrlMd5:String, jobUrl: String,
                     jobName:String, jobAddr:String, jobDesc:String, salaryYearLow: String, salaryYearHigh:String, welfare:String,
                     education:String, workYears:String, age:String, language:String,
                     companyName:String, companyType:String, companyAddr:String, companySize:String, companyDesc:String,
                     publishDate:String, crawlDate:String)
