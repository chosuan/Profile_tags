package cn.itcast.tags.test.hbase.tools

import cn.itcast.tags.tools.HBaseTools
import org.apache.spark.sql.{DataFrame, SparkSession}

object HBaseToolsTest {
	
	def main(args: Array[String]): Unit = {
		
		val spark = SparkSession.builder()
			.appName(this.getClass.getSimpleName.stripSuffix("$"))
			.master("local[4]")
			.config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
			.getOrCreate()
		import spark.implicits._
		
		// TODO: 调用HBaseTools工具类，加载HBase表中的数据
		/*
			spark.read
				.format("hbase")
				.option("zkHosts", "bigdata-cdh01.itcast.cn")
				.option("zkPort", "2181")
        		.load()
		 */
		val dataframe: DataFrame = HBaseTools.read(
			spark, "bigdata-cdh01.itcast.cn", "2181", //
			"tbl_tag_users", "detail", Seq("id", "gender") //
		)
		dataframe.printSchema()
		dataframe.show(100, truncate = false)
		
		// TODO: 调用HBaseTools工具类，保存数据至HBase表
		/*
			dataframe.write
				.format("hbase")
				.option("zkHosts", "bigdata-cdh01.itcast.cn")
				.option("zkPort", "2181")
        		.save()
		 */
		HBaseTools.write(
			dataframe, "bigdata-cdh01.itcast.cn", "2181", //
			"tbl_users", "info", "id"
		)
		
		// 应用结束，关闭资源
		spark.stop()
		
	}
	
}
