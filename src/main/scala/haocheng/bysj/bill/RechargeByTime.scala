package haocheng.bysj.bill

import java.io.FileReader
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object RechargeByTime {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
//      .set("spark.defalut.parallelism","1")
    val session = SparkSession.builder()
      .config(conf)
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    /*读取hdfs数据源*/
    val inputPath = "hdfs://192.168.23.11:9000/bysj/bill/2018-06-09/"
    val DF: DataFrame = session.read.json(inputPath)

    /*创建临时视图*/
    DF.createTempView("res_recharge_time")

    /*执行sql语句*/
    val frame: DataFrame = session.sql("SELECT HOUR(rechargetime) hour,sum(CASE WHEN MONTH(rechargetime) = 3 then 1 else 0 end ) March,sum(CASE WHEN MONTH(rechargetime) = 4 then 1 else 0 end ) April,sum(CASE WHEN MONTH(rechargetime) = 5 then 1 else 0 end ) May,COUNT(*) total from res_recharge_time GROUP BY hour ORDER BY hour").coalesce(1)
//    val frame: DataFrame = session.sql("select DATE_FORMAT(rechargetime,'m') month, DATE_FORMAT(rechargetime,'H') hour,count(*) total from res_recharge_time group by hour order by cast(hour as int)").coalesce(1)
//    val frame: DataFrame = session.sql("SELECT HOUR(rechargetime) as HOUR ,count(*) from res_recharge_time ORDER BY HOUR")
      /*SELECT HOUR(rechargetime) hour,
        sum(CASE WHEN MONTH(rechargetime) = 3 then 1 else 0 end ) March,
        sum(CASE WHEN MONTH(rechargetime) = 4 then 1 else 0 end ) April,
        sum(CASE WHEN MONTH(rechargetime) = 5 then 1 else 0 end ) May,
        COUNT(*) total from res_recharge_time GROUP BY hour
       */

    /*获取数据库的连接*/
    val connprop: Properties = new Properties()
    val reader: FileReader = new FileReader("F:\\workspace\\IDEA_U\\telcoAnalyze\\src\\main\\resources\\jdbc.properties")
    connprop.load(reader)
    val url = "jdbc:mysql://127.0.0.1:3306/bysj?characterEncoding=utf8"

    /*将结果写入mysql*/
    frame.write.mode("overwrite")jdbc(url,"res_recharge_time",connprop)
  }

}
