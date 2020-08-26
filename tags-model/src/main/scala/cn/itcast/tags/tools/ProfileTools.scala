package cn.itcast.tags.tools

import cn.itcast.tags.config.ModelConfig
import org.apache.spark.sql.{DataFrame, SaveMode, SparkSession}
import org.apache.spark.sql.functions._

/**
 * 构建画像标签数据工具类：加载（读取）、保存（写入）及合并画像标签
 */
object ProfileTools {

    /**
     * 从HBase表中加载画像标签数据
     * @param spark SparkSession实例对象
     */
    def loadProfile(spark: SparkSession): DataFrame = {
        /*HBaseTools.read(
            spark, //
            ModelConfig.PROFILE_TABLE_ZK_HOSTS, //
            ModelConfig.PROFILE_TABLE_ZK_PORT, //
            ModelConfig.PROFILE_TABLE_NAME, //
            ModelConfig.PROFILE_TABLE_FAMILY_USER, //
            ModelConfig.PROFILE_TABLE_SELECT_FIELDS.split(",") //
        )*/

        spark.read
            .format("hbase")
            .option("zkHosts", ModelConfig.PROFILE_TABLE_ZK_HOSTS)
            .option("zkPort", ModelConfig.PROFILE_TABLE_ZK_PORT)
            .option("hbaseTable", ModelConfig.PROFILE_TABLE_NAME)
            .option("family", ModelConfig.PROFILE_TABLE_FAMILY_USER)
            .option("selectFields",ModelConfig.PROFILE_TABLE_SELECT_FIELDS)
            .load()
    }

    /**
     * 将每个标签模型计算用户标签与历史画像标签数据合并
     * @param modelDF 标签模型计算标签数据，用户ID字段名称为uid，标签ID：tagId
     * @param profileDF 历史画像标签数据，用户ID字段名称为userId，画像标签：tagIds
     * @return 合并后画像标签数据，字段信息为：userId和tagIds
     */
    def mergeProfileTags(modelDF: DataFrame, profileDF: DataFrame): DataFrame = {
        import modelDF.sparkSession.implicits._

        // a. 依据用户ID关联标签数据
        val mergeDF: DataFrame = modelDF
            // 按照模型数据中userId与画像数据中rowKey关联
            .join(profileDF, modelDF("uid") === profileDF("userId"), "left")

        // b. 自定义UDF函数，合并已有标签与计算标签
        val merge_tags_udf = udf(
            (tagId: String, tagIds: String) => {
                tagIds.split(",")
                    .:+(tagId).distinct // 合并和去重
                    .mkString(",")
            }
        )
        // c. 合并标签数据
        val newProfileDF: DataFrame = mergeDF.select(
            $"uid".as("userId"), //
            when($"tagIds".isNull, $"tagId") //
                .otherwise(merge_tags_udf($"tagId", $"tagIds")) //
                .as("tagIds")//
        )
        // d. 返回标签画像数据
        newProfileDF
    }

    /**
     * 合并每个标签模型计算用户标签与历史画像标签数据
     * @param modelDF 标签数据，字段为uid和tagId
     * @param profileDF 画像标签数据，字段为userId和tagIds
     * @param ids 标签所有ID
     * @return
     */
    def mergeProfileTags(modelDF: DataFrame, profileDF: DataFrame,
                         ids: Set[String]): DataFrame = {
        import modelDF.sparkSession.implicits._

        // a. 依据用户ID关联标签数据
        val mergeDF: DataFrame = modelDF
            // 按照模型数据中userId与画像数据中rowKey关联
            .join(profileDF, modelDF("uid") === profileDF("userId"), "left")

        // b. 自定义UDF函数，合并已有标签与计算标签
        val merge_tags_udf = udf(
            (tagId: String, tagIds: String) => {
                if(null != tagIds){
                    // i. 画像标签Set集合
                    val tagIdsSet: Set[String] = tagIds.split(",").toSet
                    // ii. 交集
                    val interSet: Set[String] = tagIdsSet & ids
                    // iii. 合并新标签
                    val newTagIds: Set[String] = tagIdsSet -- interSet + tagId
                    // iv. 返回标签
                    newTagIds.mkString(",")
                }else{
                    tagId
                }
            }
        )
        // c. 合并标签数据
        val newProfileDF: DataFrame = mergeDF.select(
            $"uid".as("userId"), //
            merge_tags_udf($"tagId", $"tagIds").as("tagIds")//
        )

        // d. 返回标签画像数据
        newProfileDF
    }


    /**
     * 将画像标签数据保存到HBase表中
     * @param profileDF 画像标签数据
     */
    def saveProfile(profileDF: DataFrame): Unit = {
        /*HBaseTools.write(
            profileDF,
            ModelConfig.PROFILE_TABLE_ZK_HOSTS, //
            ModelConfig.PROFILE_TABLE_ZK_PORT, //
            ModelConfig.PROFILE_TABLE_NAME, //
            ModelConfig.PROFILE_TABLE_FAMILY_USER, //
            ModelConfig.PROFILE_TABLE_ROWKEY_COL //
        )*/

        profileDF.write
            .mode(SaveMode.Overwrite)
            .format("hbase")
            .option("zkHosts", ModelConfig.PROFILE_TABLE_ZK_HOSTS)
            .option("zkPort", ModelConfig.PROFILE_TABLE_ZK_PORT)
            .option("hbaseTable", ModelConfig.PROFILE_TABLE_NAME)
            .option("family", ModelConfig.PROFILE_TABLE_FAMILY_USER)
            .option("rowKeyColumn", ModelConfig.PROFILE_TABLE_ROWKEY_COL)
            .save()
    }

}
