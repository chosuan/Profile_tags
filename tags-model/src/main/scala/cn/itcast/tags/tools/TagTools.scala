package cn.itcast.tags.tools

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.{col, udf}
import org.apache.spark.sql.types.StringType

/**
 * 针对标签进行相关操作工具类
 */
object TagTools {

    /**
     * 将[属性标签]数据中标签ID提取并转换为为Set集合
     * @param tagDF 属性标签数据
     * @return Set 集合
     */
    def convertSet(tagDF: DataFrame): Set[String] = {
        import tagDF.sparkSession.implicits._
        tagDF
            // 获取属性标签数据
            .filter($"level" === 5)
            .rdd // 转换为RDD
            .map{row => row.getAs[Long]("id")}
            .collect()
            .map(_.toString)
            .toSet
    }

    /**
     * 将[属性标签]数据中规则：rule与标签ID：tagId转换为Map集合
     * @param tagDF 属性标签数据
     * @return Map 集合
     */
    def convertMap(tagDF: DataFrame): Map[String, Long] = {
        import tagDF.sparkSession.implicits._
        tagDF
            // 获取属性标签数据
            .filter($"level" === 5)
            // 选择标签规则rule和标签Id
            .select($"rule", $"id".as("tagId"))
            // 转换为Dataset
            .as[(String, Long)]
            // 转换为RDD
            .rdd
            // 转换为Map集合
            .collectAsMap().toMap
    }

    /**
     * 依据[标签业务字段的值]与[标签规则]匹配，进行打标签（userId, tagId)
     * @param dataframe 标签业务数据
     * @param field 标签业务字段
     * @param tagDF 标签数据
     * @return 标签模型数据
     */
    def ruleMatchTag(dataframe: DataFrame, field: String,
                     tagDF: DataFrame): DataFrame = {
        val spark: SparkSession = dataframe.sparkSession
        import spark.implicits._
        // 1. 获取规则rule与tagId集合
        val ruleTagMap: Map[String, Long] = convertMap(tagDF)
        // 2. 将Map集合数据广播出去
        val ruleTagMapBroadcast: Broadcast[Map[String, Long]] = spark.sparkContext.broadcast(ruleTagMap)

        println(ruleTagMapBroadcast.value)
        // 3. 自定义UDF函数, 依据Job职业和属性标签规则进行标签化
        val field_to_tag: UserDefinedFunction = udf(
            (field: String) => ruleTagMapBroadcast.value(field)
        )
        // 4. 计算标签，依据业务字段值获取标签ID
        val modelDF: DataFrame = dataframe
            .select(
                $"id".as("uid"), //
                field_to_tag(col(field)).cast(StringType).as("tagId")
            )
        //modelDF.printSchema()
        //modelDF.show(50, truncate = false)

        // 5. 返回计算标签数据
        modelDF
    }

    /**
     *  将标签数据中属性标签规则rule拆分为范围: start, end
     *
     * @param tagDF 标签数据
     * @return 数据集DataFrame
     */
    def convertTuple(tagDF :DataFrame): DataFrame = {
        // 导入隐式转换和函数库
        import tagDF.sparkSession.implicits._
        // 1. 自定UDF函数，解析分解属性标签的规则rule： 19500101-19591231
        val rule_to_tuple: UserDefinedFunction = udf(
            (rule: String) => {
                val Array(start, end) = rule.split("-").map(_.toInt)
                // 返回二元组
                (start, end)
            }
        )

        val ruleDF: DataFrame = tagDF
            .filter($"level" === 5) // 5级标签
            .select(
                $"id".as("tagId"), //
                rule_to_tuple($"rule").as("rules") //
            )
            // 获取起始start和结束end
            .select(
                $"tagId", //
                $"rules._1".as("start"), //
                $"rules._2".as("end") //
            )

//        ruleDF.show(20, truncate = false)
        // 3. 返回标签规则
        ruleDF
    }

}
