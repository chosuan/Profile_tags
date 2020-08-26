package cn.itcast.tags.test.hbase.sql

import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}

object HBaseSQLSourceTest {
    def main(args: Array[String]): Unit = {

        val spark = SparkSession
            .builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[4]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()

        import spark.implicits._

        val usersDF: DataFrame = spark.read
            .format("hbase")
            .option("zkHosts", "bigdata-cdh01.itcast.cn")
            .option("zkPort", "2181")
            .option("hbaseTable", "tbl_tag_users")
            .option("family", "detail")
            .option("selectFields", "id,gender")
            .load()

        usersDF.printSchema()
        usersDF.show(10, truncate = false)
        usersDF.cache()

        usersDF.write
            .mode(SaveMode.Append)
            .format("hbase")
            .option("zkHosts", "bigdata-cdh01.itcast.cn")
            .option("zkPort", "2181")
            .option("hbaseTable", "tbl_users")
            .option("family", "info")
            .option("rowKeyColumn", "id")
            .save()

    }
}
