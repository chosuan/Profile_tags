package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, BasicModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

class NationalityModel extends AbstractModel("国籍标签",ModelType.MATCH){
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {

        val modelDF: DataFrame = TagTools.ruleMatchTag(
            businessDF, "nationality", tagDF
        )
        modelDF.printSchema()
        modelDF

    }
}

object NationalityModel{
    def main(args: Array[String]): Unit = {
        val tagModel = new NationalityModel()
        tagModel.executeModel(329L)
    }
}
