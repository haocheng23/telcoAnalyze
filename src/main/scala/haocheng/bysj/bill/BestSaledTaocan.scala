package haocheng.bysj.bill

import java.io.FileReader
import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object BestSaledTaocan {

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
    DF.createTempView("res_taocan")

    /*执行sql语句*/
    val frame: DataFrame = session.sql("select taocan,count(taocan) as total from res_taocan group by taocan order by taocan asc")

    frame.foreach(t=>{
      println(t)
    })

    /*获取数据库的连接*/
    val connprop: Properties = new Properties()
    val reader: FileReader = new FileReader("F:\\workspace\\IDEA_U\\telcoAnalyze\\src\\main\\resources\\jdbc.properties")
    connprop.load(reader)
    val url = "jdbc:mysql://127.0.0.1:3306/bysj?characterEncoding=utf8"

    /*将结果写入mysql*/
    frame.write.mode("overwrite")jdbc(url,"res_taocan",connprop)
    //    result.coalesce(1).write.mode("overwrite")jdbc("jdbc:mysql://127.0.0.1:3306/result","taoCan",prop)

  }

}
