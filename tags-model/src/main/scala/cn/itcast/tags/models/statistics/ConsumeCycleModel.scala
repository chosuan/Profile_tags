package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StringType

class ConsumeCycleModel extends AbstractModel("消费周期标签", ModelType.STATISTICS) {
    /*
        344	消费周期		selectFieldNames=memberid,finishtime
            345	近7天		0-7
            346	近2周		8-14
            347	近1月		15-30
            348	近2月		31-60
            349	近3月		61-90
            350	近4月		91-120
            351	近5月		121-150
            352	近半年		151-180
     */
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {

        import businessDF.sparkSession.implicits._
        // 1. 属性标签数据转换，最终DataFrame包含字段: tagId、start、end
        val ruleDF: DataFrame = TagTools.convertTuple(tagDF)
        /*
        |tagId|start|end|
        +-----+-----+---+
        |345  |0    |7  |
         */
        //        ruleDF.show(10, truncate = false)

        // 2. 业务数据处理，通过每个用户的订单数据计算出最近订单距今天数
        val daysDF: DataFrame = businessDF
            // i. 按照用户ID分组，获取最近订单，finishtime最大值的订单
            .groupBy($"memberid")
            .agg(max($"finishtime").as("max_finishtime"))
            // ii. 转换finishtime为日期时间格式
            .select(
                $"memberid".as("uid"), //
                from_unixtime($"max_finishtime").as("finish_time"), //
                current_timestamp().as("now_time")
            )
            // iii. 计算天数
            .select(
                $"uid",  //
                datediff($"now_time", $"finish_time").as("days")
            )

        //        daysDF.printSchema()         |13822861|375 |
//                daysDF.show(20,false)

        // 3. 依据业务数据和标签数据，打标签
        val modelDF: DataFrame = daysDF
            .join(ruleDF)
            .where(
                daysDF("days").between(ruleDF("start"), ruleDF("end"))
            )
            .select($"uid", $"tagId".cast(StringType))
//        modelDF.show(5,false)

        modelDF
    }
}

object ConsumeCycleModel {
    def main(args: Array[String]): Unit = {

        val modelTag = new ConsumeCycleModel
        modelTag.executeModel(344L)

    }
}
