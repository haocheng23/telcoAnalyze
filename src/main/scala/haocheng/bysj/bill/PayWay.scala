package haocheng.bysj.bill

import java.io.FileReader
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object PayWay {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val session = SparkSession.builder()
      .config(conf)
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    val inputPath = "hdfs://192.168.23.11:9000/bysj/bill/2018-06-09/"
    val DF: DataFrame = session.read.json(inputPath)
    DF.createTempView("res_payway")
    val frame: DataFrame = session.sql("SELECT payway,COUNT(payway) total FROM res_payway GROUP BY payway ORDER BY total DESC")
    //SELECT payWay,COUNT(payWay) total FROM res_payway GROUP BY payWay ORDER BY total DESC

    val connprop: Properties = new Properties()
    val reader: FileReader = new FileReader("F:\\workspace\\IDEA_U\\telcoAnalyze\\src\\main\\resources\\jdbc.properties")
    connprop.load(reader)
    val url = "jdbc:mysql://127.0.0.1:3306/bysj?characterEncoding=utf8"

    frame.write.mode("overwrite")jdbc(url,"res_payway",connprop)
  }

}
