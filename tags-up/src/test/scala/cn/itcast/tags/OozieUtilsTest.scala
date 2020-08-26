package cn.itcast.tags

import cn.itcast.tags.up.OozieParam
import cn.itcast.tags.up.OozieUtils.{genProperties, start, store, uploadConfig}

object OozieUtilsTest {
	/**
	 * 调用方式展示
	 */
	def main(args: Array[String]): Unit = {

		val param = OozieParam(
			0,
			"org.apache.spark.examples.SparkPi",
			"hdfs://bigdata-cdh01.itcast.cn:8020/apps/tags/models/tag_0/lib/model.jar",
			"",
			"2020-08-22T22:25+0800",
			"2020-08-22T22:28+0800"
		)
		val prop = genProperties(param)
		uploadConfig(param.modelId)
		store(param.modelId, prop)
		start(prop)
	}
}
