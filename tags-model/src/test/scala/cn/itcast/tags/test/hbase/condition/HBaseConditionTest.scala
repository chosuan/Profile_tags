package cn.itcast.tags.test.hbase.condition

import cn.itcast.tags.meta.HBaseMeta
import org.apache.spark.sql.{DataFrame, SparkSession}

object HBaseConditionTest {

    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession
            .builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[4]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .getOrCreate()
        import spark.implicits._
        import org.apache.spark.sql.functions._

        // 1. 从HBase表读取数据规则集合Map
        val ruleMap: Map[String, String] = Map(
            "inType"-> "hbase",
            "zkHosts"-> "bigdata-cdh01.itcast.cn",
            "zkPort"-> "2181",
            "hbaseTable"-> "tbl_profile_users",
            "family"-> "detail",
            "selectFieldNames"-> "id,gender",
            "whereCondition"-> "modified#day#30"
        )

        // 2. 规则数据封装到HBaseMeta中
        val hbaseMeta: HBaseMeta = HBaseMeta.getHBaseMeta(ruleMap)
        //
//        println(hbaseMeta)

        // 3. SparkSQL从HBase表读取数据
        val usersDF: DataFrame = spark.read
            .format("hbase")
            .option("zkHosts", hbaseMeta.zkHosts)
            .option("zkPort", hbaseMeta.zkPort)
            .option("hbaseTable", hbaseMeta.hbaseTable)
            .option("family", hbaseMeta.family)
            .option("selectFields", hbaseMeta.selectFieldNames)
            .option("filterConditions", hbaseMeta.filterConditions)
            .load()

        usersDF.printSchema()
        usersDF.show(100, truncate = false)
        println(s"count = ${usersDF.count()}")

        // 应用结束，关闭资源
        spark.stop()

    }

}
