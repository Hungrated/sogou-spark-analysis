package com.zjuhungrated.sparkanalysis

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.storage.StorageLevel

object SogouAnalysis {

  private val INPUT_PATH = "hdfs://localhost:9000/sogouanalysis/input/sogou.full.utf8"

  private val OUTPUT_PATH = "result"

  private val helper = new SogouAnalysisHelper

  def main(args: Array[String]): Unit = {

    //实例化configuration，用于编辑我们任务的相关信息
    val conf = new SparkConf().setAppName("SogouSparkAnalysis-Count").setMaster("local")

    // sc是Spark Context，指的是“上下文”，也就是运行环境
    val sc = new SparkContext(conf)

    //通过sc获取一个（HDFS上的）文本文件
    val raw = sc.textFile(INPUT_PATH)

    // 数据去空与扩展预处理
    val rdd = raw.map(x => x.split("\t"))
      .filter(_.length == 6)
      .map(x => x ++ Array[String](x(0).slice(0, 4), x(0).slice(4, 6), x(0).slice(6, 8), x(0).slice(8, 10)))
      .persist(StorageLevel.MEMORY_ONLY)

    helper.deleteDirIfExists(OUTPUT_PATH)

    /**
      * 1 对搜索日志进行关键词频统计
      */
    rdd.map(x => (x(2), 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .saveAsTextFile(OUTPUT_PATH + "/01")
    /**
      * 2 独立用户数量统计
      */
    val uidCount = rdd.map(x => (x(1), 1))
      .reduceByKey((x, y) => x + y)
      .count()
    println(uidCount)

    /**
      * 3 比较各小时总搜索量
      */
    rdd.map(x => (x(9), 1))
      .reduceByKey((x, y) => x + y)
      .sortByKey()
      .saveAsTextFile(OUTPUT_PATH + "/03")
    //      .collect()
    //      .foreach(x => println(x._1 + "\t" + x._2))

    /**
      * 4 比较各小时用户平均搜索量
      */
    rdd.map(x => ((x(9), x(1)), 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._1._1, x._2))
      .combineByKey(
        v => (v, 1),
        (acc: (Int, Int), v) => (acc._1 + v, acc._2 + 1),
        (acc1: (Int, Int), acc2: (Int, Int)) => (acc1._1 + acc2._1, acc1._2 + acc2._2)
      )
      .map { case (key, value) => (key, value._1 / value._2.toFloat) }
      .sortByKey()
      .saveAsTextFile(OUTPUT_PATH + "/04")
    //      .collect()
    //      .foreach(x => println(x._1 + "\t" + x._2))

    /**
      * 5 求独立用户搜索量并按此排序
      */
    rdd.map(x => (x(1), 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .saveAsTextFile(OUTPUT_PATH + "/05")

    /**
      * 6 求用户点击序号与搜索结果排序均为1的URL并按点击次数排序
      */
    rdd.filter(_ (3).toInt == 1)
      .filter(_ (4).toInt == 1)
      .map(x => (x(5), 1))
      .reduceByKey((x, y) => x + y)
      .map(x => (x._2, x._1))
      .sortByKey(false)
      .map(x => (x._2, x._1))
      .saveAsTextFile(OUTPUT_PATH + "/06")

    sc.stop()

  }

}
