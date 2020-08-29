package cn.itcast.tags.models.statistics

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.expressions.Window
/**
 * 标签模型开发：支付方式标签模型
 */
class PayTypeModel extends AbstractModel("支付方式标签",ModelType.STATISTICS){
/*
    353	支付方式		 selectFieldNames=memberid,paymentcode
            354	支付宝		alipay
            355	微信支付		wxpay
            356	银联支付		chinapay
            357	货到付款		cod
 */
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {

//        businessDF.show(10,false)
        /*
        +--------+-----------+
        |memberid|paymentcode|
        +--------+-----------+
        |912     |alipay     |
        |528     |alipay     |
        |361     |alipay     |
         */
        // 导入隐式转换和函数库
        import businessDF.sparkSession.implicits._
        import org.apache.spark.sql.functions._

        // 1. 业务数据转换：id, paymentcode（使用最多的）
        val payDF: DataFrame = businessDF
            // 按照用户ID和支付方式分组，统计次数
            .groupBy($"memberid", $"paymentcode")
            .count()
            // 添加一列，使用窗口分析函数
            .withColumn(
                "rnk", //
                row_number().over(
                    Window.partitionBy($"memberid").orderBy($"count".desc)
                )
            )
            // 过滤
            .where($"rnk".equalTo(1))
            // 选取字段
            .select($"memberid".as("id"), $"paymentcode")

        // 2. 结合属性标签数据，打标签
        val modelDF: DataFrame = TagTools.ruleMatchTag(
            payDF, "paymentcode", tagDF
        )
//        modelDF.show(20,false)


        modelDF

    }
}

object PayTypeModel{
    def main(args: Array[String]): Unit = {

        val ModelTag = new PayTypeModel
        ModelTag.executeModel(353L)

    }
}
