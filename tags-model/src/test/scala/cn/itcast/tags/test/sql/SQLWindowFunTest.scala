package cn.itcast.tags.test.sql

import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.{DataFrame, SparkSession}

object SQLWindowFunTest {

    def main(args: Array[String]): Unit = {

        System.load("D:\\Program Files\\hadoop-2.6.0\\bin\\hadoop.dll")

        val spark = SparkSession.builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[4]")
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            // 设置Shuffle分区数目
            .config("spark.sql.shuffle.partitions", "4")
            // 设置与Hive集成: 读取Hive元数据MetaStore服务
            .config("hive.metastore.uris", "thrift://bigdata-cdh01.itcast.cn:9083")
            // 设置数据仓库目录
            .config("spark.sql.warehouse.dir", "hdfs://bigdata-cdh01.itcast.cn:8020/user/hive/warehouse")
            .enableHiveSupport()
            .getOrCreate()
        import org.apache.spark.sql.functions._
        import spark.implicits._

        // 2. 加载Hive表中emp数据：db_hive.emp
        val empDF: DataFrame = spark.read
            .table("db_hive.emp")

//        empDF.printSchema()
//        empDF.show(20, false)

        // 3. TODO: 获取每个部分员工工资最高的人员信息 -> empno, ename, sal, deptno
        // 3.1 使用SQL分析
        spark.sql(
            """
            |WITH tmp AS(
            |SELECT
            |  empno, ename, sal, deptno,
            |  ROW_NUMBER() OVER(PARTITION BY deptno ORDER BY sal DESC) AS rnk
            |FROM
            |  db_hive.emp
            |)
            |SELECT t.empno, t.ename, t.sal, t.deptno FROM tmp t WHERE t.rnk = 1
            |""".stripMargin)
            .show(10, truncate = false)

        println("=================================================================")

        val resultDF: DataFrame = empDF
            .withColumn(
                "rnk", //
                row_number().over(
                    Window.partitionBy($"deptno").orderBy($"sal".desc)
                )
            )
            .where($"rnk".equalTo(1))
            .select($"empno", $"ename", $"sal", $"deptno")

        resultDF.show(20,false)

        spark.stop()

    }
}
