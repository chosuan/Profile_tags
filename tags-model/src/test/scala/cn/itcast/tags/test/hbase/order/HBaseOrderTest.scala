package cn.itcast.tags.test.hbase.order

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.storage.StorageLevel

object HBaseOrderTest {

    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[4]")
            .config("spark.sql.shuffle.partitions","4")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        import org.apache.spark.sql.functions._
        import spark.implicits._

        // 读取数据
        val ordersDF: DataFrame = spark.read
            .format("hbase")
            .option("zkHosts", "bigdata-cdh01.itcast.cn")
            .option("zkPort", "2181")
            .option("hbaseTable", "tbl_tag_orders")
            .option("family", "detail")
            .option("selectFields", "memberid,paymentcode")
            .load()
        ordersDF.printSchema()
        ordersDF.persist(StorageLevel.MEMORY_AND_DISK)
//        ordersDF.show(50, truncate = false)

        val cnt: Long = ordersDF
            .agg(
                countDistinct($"paymentcode").as("total")
            )
            .head()
            .getAs[Long]("total")

        println(s"PayMentCode = $cnt")

        spark.stop()

    }

}
