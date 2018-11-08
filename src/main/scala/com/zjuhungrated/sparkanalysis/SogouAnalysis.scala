package com.zjuhungrated.sparkanalysis

import org.apache.spark.storage.StorageLevel
import org.apache.spark.{SparkConf, SparkContext}

object SogouAnalysis {

  private val INPUT_PATH = "hdfs://localhost:9000/sogouanalysis/input/sogou.full.utf8.ext"

  private val OUTPUT_PATH = "result"

  private val helper = new SogouAnalysisHelper

  def main(args: Array[String]): Unit = {

    //实例化configuration，用于编辑我们任务的相关信息
    val conf = new SparkConf().setAppName("SogouSparkAnalysis-Count").setMaster("local")

    // sc是Spark Context，指的是“上下文”，也就是我们运行的环境，需要把conf当参数传进去
    val sc = new SparkContext(conf)

    //通过sc获取一个（HDFS上的）文本文件
    val raw = sc.textFile(INPUT_PATH).persist(StorageLevel.MEMORY_ONLY)

    // 数据去空预处理
    val rdd = raw.map(_.split("\t")).filter(_.length == 10)

    helper.deletDirIfExists(OUTPUT_PATH)

    /**
      * 1 对搜索日志进行关键词频统计（OK）
      */
    //    rdd.map(x => (x(2), 1))
    //      .reduceByKey((x, y) => x + y)
    //      .map(x => (x._2, x._1))
    //      .sortByKey(false)
    //      .map(x => (x._2, x._1))
    //      .saveAsTextFile(OUTPUT_PATH + "/01")

    /**
      * 2 求独立用户搜索关键词次数
      */
    // 用一下combine aggregate
    //    rdd.map(x => ("sum", x(1).split("").length))
    //      .reduceByKey((x, y) => x + y)
    //      .map(x =>)
    //      .saveAsTextFile(OUTPUT_PATH + "/02")
    /**
      * 3 比较各小时搜索量（OK）
      */
    //    rdd.map(x => (x(9), 1))
    //      .reduceByKey((x, y) => x + y)
    //      .sortByKey()
    //      .saveAsTextFile(OUTPUT_PATH + "/03")
    /**
      * 4 求独立用户搜索量并按此排序
      */
    //    rdd.map(x => (x(1), 1))
    //      .reduceByKey((x, y) => x + y)
    //      .map(x => (x._2, x._1))
    //      .sortByKey(false)
    //      .map(x => (x._2, x._1))
    //      .saveAsTextFile(OUTPUT_PATH + "/05")
    /**
      * 5 求用户点击序号与搜索结果排序均为1的URL并按点击次数排序（OK）
      */
    //    rdd.filter(_ (3).toInt == 1)
    //      .filter(_ (4).toInt == 1)
    //      .map(x => (x(5), 1))
    //      .reduceByKey((x, y) => x + y)
    //      .map(x => (x._2, x._1))
    //      .sortByKey(false)
    //      .map(x => (x._2, x._1))
    //      .saveAsTextFile(OUTPUT_PATH + "/04")
    /**
      * 6 求用户点击URL为搜索结果前10的占比
      */
    // rdd

    sc.stop()

  }

}
