package haocheng.bysj.bill

import java.io.FileReader
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}

object Bill {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val session = SparkSession.builder()
      .config(conf)
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    /*读取hdfs数据源*/
    val inputPath = "hdfs://192.168.23.11:9000/bysj/bill/2018-06-09/"
    val dataframe: DataFrame = session.read.json(inputPath)

    /**
      * 指定部分字段的schema信息
      */
    //dataframe转RDD
    val rdd: RDD[Row] = dataframe.rdd
    //取出相应的字段
    val rowRDD = rdd.map(t => Row(t.get(0).toString.toLong, t.get(1).toString.toLong, t.get(2).toString.toLong, t.get(3), t.get(4), t.get(5), t.get(6), t.get(7), t.get(8)))
    //设置新的schema信息
    val schema = StructType(
      List(
        StructField("calltime", LongType, true),
        StructField("flow", LongType, true),
        StructField("messagenum", LongType, true),
        StructField("payway", StringType, true),
        StructField("phonenumber", StringType, true),
        StructField("province", StringType, true),
        StructField("rechargeTime", StringType, true),
        StructField("rechargefee", StringType, true),
        StructField("taocan", StringType, true)
      )
    )
    //将schema信息应用到rowRDD上
    val newDF = session.createDataFrame(rowRDD, schema)

//    newDF.show(10)
//    newDF.printSchema()

    /*创建临时视图*/
    newDF.createTempView("res_bill")

    /*执行sql语句*/
    val frame: DataFrame = session.sql("SELECT * from res_bill ")

    /*获取数据库的连接*/
    val connprop: Properties = new Properties()
    val reader: FileReader = new FileReader("F:\\workspace\\IDEA_U\\telcoAnalyze\\src\\main\\resources\\jdbc.properties")
    connprop.load(reader)
    val url = "jdbc:mysql://127.0.0.1:3306/bysj?characterEncoding=utf8"

    /*将结果写入mysql*/
    frame.write.mode("overwrite")jdbc(url,"res_bill",connprop)
  }

}
