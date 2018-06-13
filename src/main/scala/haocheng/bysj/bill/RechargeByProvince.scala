package haocheng.bysj.bill

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object RechargeByProvince {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val session = SparkSession.builder()
      .config(conf)
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    /*读取hdfs数据源*/
    val inputPath = "hdfs://192.168.23.11:9000/bysj/bill/2018-06-09/"
    val DF: DataFrame = session.read.json(inputPath)

    /*创建临时视图*/
    DF.createTempView("res_recharge_province")

    /*执行sql语句*/
    val frame: DataFrame = session.sql("SELECT province,COUNT(*) total FROM res_recharge_province GROUP BY province ORDER BY cast(total as int) DESC").coalesce(1)

    /*获取数据库的连接*/
    val connprop: Properties = new Properties()
    connprop.put("user","root");
    connprop.put("password","123456");
    val url = "jdbc:mysql://127.0.0.1:3306/bysj?characterEncoding=utf8"

    /*将结果写入mysql*/
    frame.write.mode("overwrite")jdbc(url,"res_recharge_province",connprop)
  }

}
