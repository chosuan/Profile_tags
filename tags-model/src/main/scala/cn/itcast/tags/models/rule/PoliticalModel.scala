package cn.itcast.tags.models.rule

import cn.itcast.tags.models.BasicModel
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

/**
 * 标签模型开发：用户政治面貌标签模型
 */
class PoliticalModel extends BasicModel{

    /*
    328	政治面貌
      329	群众		1
      330	党员		2
      331	无党派人士		3
     */
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
        // 计算标签，由于是规则匹配类型标签计算，直接调用工具类TagTools
        val modelDF: DataFrame = TagTools.ruleMatchTag(
            businessDF, "politicalface", tagDF
        )
        //modelDF.printSchema()
        //modelDF.show(200, truncate = false)
        // 返回标签数据
        modelDF
    }
}

object PoliticalModel{
    def main(args: Array[String]): Unit = {
        val tagModel = new PoliticalModel()
        tagModel.executeModel(325L)
    }
}
