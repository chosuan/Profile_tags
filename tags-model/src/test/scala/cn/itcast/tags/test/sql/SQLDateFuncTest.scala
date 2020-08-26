package cn.itcast.tags.test.sql

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object SQLDateFuncTest {
    def main(args: Array[String]): Unit = {

        val spark: SparkSession = SparkSession.builder()
            .appName(this.getClass.getSimpleName.stripSuffix("$"))
            .master("local[4]")
            .getOrCreate()
        import spark.implicits._

        //模拟数据
        val dataframe: DataFrame = Seq(
            "1589817600" // 2020-05-19 00:00:00
        ).toDF("finishtime")
        /*
        +-------------------+-----------------------+----+
        |finish_time        |now_time               |days|
        +-------------------+-----------------------+----+
        |2020-05-19 00:00:00|2020-08-26 10:46:28.614|99  |
        +-------------------+-----------------------+----+
         */
        dataframe.select(
            // 转化数据格式
            $"finishtime", //
            from_unixtime($"finishtime").as("finish_time"), //
            current_timestamp().as("now_time")
        )
            .select(
                $"finish_time", $"now_time", //
                datediff($"now_time", $"finish_time").as("days")
            ).show(false)

        println("======================================================")
        /*
        +-----------+----------+----+
        |finish_date|now_date  |days|
        +-----------+----------+----+
        |2020-05-19 |2020-08-26|99  |
        +-----------+----------+----+
         */
        dataframe.select(
            // 转化数据格式
            $"finishtime", //
            to_date(from_unixtime($"finishtime")).as("finish_date"), //
            current_date().as("now_date")
        )
            .select(
                $"finish_date", $"now_date", //
                datediff($"now_date", $"finish_date").as("days")
            ).show(false)

    }
}
