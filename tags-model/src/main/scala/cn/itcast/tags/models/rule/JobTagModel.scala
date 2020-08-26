package cn.itcast.tags.models.rule

import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import org.apache.spark.sql.DataFrame

/**
 * 开发标签模型应用：用户职业标签模型
 */
class JobTagModel extends AbstractModel("职业标签", ModelType.MATCH){
	/*
	321	职业
		322	学生		1
		323	公务员		2
		324	军人		3
		325	警察		4
		326	教师		5
		327	白领		6
	 */
	override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
		val modelDF = TagTools.ruleMatchTag(
			businessDF, "job", tagDF
		)
		modelDF
	}
}

object JobTagModel{
	def main(args: Array[String]): Unit = {
		val tagModel = new JobTagModel()
		tagModel.executeModel(317L, isHive = false)
	}
}