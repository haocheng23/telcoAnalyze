package haocheng.bysj.person

import java.util.Properties

import org.apache.spark.SparkConf
import org.apache.spark.sql.{DataFrame, SparkSession}

object PersonInfoVIP {
  def main(args: Array[String]): Unit = {
    val conf = new SparkConf()
    val session = SparkSession.builder()
      .config(conf)
      .appName(this.getClass.getName)
      .master("local[*]")
      .getOrCreate()
    /*读取hdfs数据源*/
    val inputPath = "hdfs://192.168.23.11:9000/bysj/person/2018-06-09/"
    val DF: DataFrame = session.read.json(inputPath)

    /*创建临时视图*/
    DF.createTempView("res_personvip")

    /*执行sql语句*/
    val frame: DataFrame = session.sql("SELECT (CASE WHEN point >= 0 AND point <=1000 THEN 'VIP0' WHEN point>1000 AND point <=2000 THEN 'VIP1' WHEN point>2000 AND point <=3000 THEN 'VIP2' WHEN point>3000 AND point <=5000 THEN 'VIP3' WHEN point>5000 AND point <=8000 THEN 'VIP4' WHEN point>8000 THEN 'VIP5' END) AS level,count(*) total,sum(case when sex='男' then 1 else 0 end ) male,sum(case when sex='女' then 1 else 0 end ) famale FROM res_personvip GROUP BY level order by level").coalesce(1)

    /*获取数据库的连接*/
    val connprop: Properties = new Properties()
    connprop.put("user", "root");
    connprop.put("password", "123456");
    val url = "jdbc:mysql://127.0.0.1:3306/bysj?characterEncoding=utf8"

    /*将结果写入mysql*/
    frame.write.mode("overwrite")jdbc(url, "res_personvip", connprop)
  }
}
