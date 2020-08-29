package cn.itcast.tags.models.ml

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import cn.itcast.tags.utils.HdfsUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.{MinMaxScaler, MinMaxScalerModel, VectorAssembler}
import org.apache.spark.ml.linalg
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DataTypes
import org.apache.spark.sql.{Column, DataFrame, SparkSession}

/**
 * 挖掘类型标签模型开发：用户客户价值标签模型（RFM模型）
 */
class RfmTagModel extends AbstractModel("客户价值标签", ModelType.ML) {

    System.load("D:\\Program Files\\hadoop-2.6.0\\bin\\hadoop.dll")

    /*
    358	客户价值
      359	  高价值		      0
      360	  中上价值	    	1
      361	  中价值		      2
      362	  中下价值	    	3
      363	  超低价值	    	4
     */
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {
//        businessDF.printSchema()
        //businessDF.show(10, truncate = 10)

        val spark: SparkSession = businessDF.sparkSession
        import spark.implicits._

        // 1）、从订单数据获取字段值，计算每个用户RFM值
        val rfmDF: DataFrame = businessDF
            // 按照用户进行分组
            .groupBy($"memberid")
            // 聚合计算R、F、M值
            .agg(
                // R 值
                max($"finishtime").as("finish_time"), //
                // F 值
                count($"ordersn").as("frequency"), //
                // M 值
                sum($"orderamount".cast(DataTypes.createDecimalType(10, 2))).as("monetary")
            )
            .select(
                $"memberid".as("uid"),
                datediff(current_timestamp(), from_unixtime($"finish_time")).as("recency"),
                $"frequency", $"monetary"
            )
        //rfmDF.printSchema()
        //rfmDF.show(130, truncate = false)

        // 2）、按照规则，给RFM值打分Score
        /*
          R: 1-3天=5分，4-6天=4分，7-9天=3分，10-15天=2分，大于16天=1分
          F: ≥200=5分，150-199=4分，100-149=3分，50-99=2分，1-49=1分
          M: ≥20w=5分，10-19w=4分，5-9w=3分，1-4w=2分，<1w=1分
         */
        // R 打分判断条件
        val rWhen: Column = when($"recency".between(1, 3), 5.0)
            .when($"recency".between(4, 6), 4.0)
            .when($"recency".between(7, 9), 3.0)
            .when($"recency".between(10, 15), 2.0)
            .when($"recency".gt(16), 1.0)
        // F 打分条件表达式
        val fWhen = when(col("frequency").between(1, 49), 1.0) //
            .when(col("frequency").between(50, 99), 2.0) //
            .when(col("frequency").between(100, 149), 3.0) //
            .when(col("frequency").between(150, 199), 4.0) //
            .when(col("frequency").geq(200), 5.0) //
        // M 打分条件表达式
        val mWhen = when(col("monetary").lt(10000), 1.0) //
            .when(col("monetary").between(10000, 49999), 2.0) //
            .when(col("monetary").between(50000, 99999), 3.0) //
            .when(col("monetary").between(100000, 199999), 4.0) //
            .when(col("monetary").geq(200000), 5.0) //
        // 基于规则对RFM值进行打分
        val rfmScoreDF: DataFrame = rfmDF
            .select(
                $"uid", rWhen.as("r_score"), //
                fWhen.as("f_score"), mWhen.as("m_score")
            )
        //rfmScoreDF.printSchema()
        // rfmScoreDF.show(30, truncate = false)

        // 3）、将RFM数据使用KMeans算法聚类（K=5个）
        // 3.1 组合r、f、m到特征向量vector中
        val assembler: VectorAssembler = new VectorAssembler()
            .setInputCols(Array("r_score", "f_score", "m_score"))
            .setOutputCol("raw_features")
        val rfmFeaturesDF: DataFrame = assembler.transform(rfmScoreDF)
        //featuresDF.printSchema()
        //featuresDF.show(10, truncate = false)

        // TODO：模型调优方式一：特征值归一化，使用最小最大值归一化
        val scalerModel: MinMaxScalerModel = new MinMaxScaler()
            .setInputCol("raw_features")
            .setOutputCol("features")
            .setMin(0.0).setMax(1.0)
            .fit(rfmFeaturesDF)
        val featuresDF: DataFrame = scalerModel.transform(rfmFeaturesDF)
//        featuresDF.printSchema()
//        featuresDF.show(20, false)

        // 3.2 使用KMeans算法，训练模型
        //        val kMeansModel: KMeansModel = trainModel(featuresDF)
        //调整超参数获取最佳模型
//        val kMeansModel: KMeansModel = trainBestModel(featuresDF)
        // 加载模型，如果存在就加载；不存在，训练获取，并保存
        val kMeansModel: KMeansModel = loadModel(featuresDF)


        // 3.3 获取类簇中心点
        val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
        // 将类簇中心点，采用拉链操作，获取索引
        val clusterIndexArray: Array[((Double, Int), Int)] = clusterCenters
            .zipWithIndex
            // 获取类簇中心点向量之和（RFM之和）
            .map { case (vector, clusterIndex) => (vector.toArray.sum, clusterIndex) }
            // rfm和降序排序
            .sortBy { case (rfm, _) => -rfm }
            // 拉链，获取索引
            .zipWithIndex
        //.foreach(println)

        // 3.4 使用模型对数据进行预测：划分类簇，属于哪个类别
        val predictionDF: DataFrame = kMeansModel.transform(featuresDF)
        //predictionDF.printSchema()
        //predictionDF.show(20, truncate = false)

        // 4）、从KMeans中获取出每个用户属于簇
        // 4.1 获取属性标签数据：rule, tagId
        val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)

        // 4.2 对类簇中心点数据进行遍历，获取对应tagId
        val clusterTagMap: Map[Int, Long] = clusterIndexArray.map { case ((_, clusterIndex), index) =>
            val tagId: Long = ruleMap(index.toString)
            // 返回 类簇中心点索引和属性标签ID
            (clusterIndex, tagId)
        }.toMap
        //clusterTagMap.foreach(println)

        // 4.3 自定义UDF函数，传递prediction，返回tagId
        val index_to_tag = udf(
            (clusterIndex: Int) => clusterTagMap(clusterIndex)
        )

        // 4.4 对预测数据，打标签
        val modelDF: DataFrame = predictionDF.select(
            $"uid", //
            index_to_tag($"prediction").as("tagId")
        )
        //modelDF.printSchema()
        //modelDF.show(100, truncate = false)

        // 5. 返回标签模型数据
        modelDF
    }


    /**
     * 使用KMeans算法训练模型
     *
     * @param dataframe 数据集
     * @return KMeansModel模型
     */
    def trainModel(dataframe: DataFrame): KMeansModel = {
        // i. 构建KMeans算法对象，设置相关参数
        val kMeans: KMeans = new KMeans()
            // 设置输入列和预测之列名称
            .setFeaturesCol("features")
            .setPredictionCol("prediction")
            .setK(5)
            .setMaxIter(20)
            .setInitMode("k-means||")
        // ii. 使用数据集训练模型
        val kMeansModel: KMeansModel = kMeans.fit(dataframe)
        // iii. 计算误差平方和SSE
        val wssse: Double = kMeansModel.computeCost(dataframe)
        println(s"WSSSE = $wssse") //  WSSSE = 0.99785407724886
        // iv. 返回模型对象
        kMeansModel
    }

    /**
     * 调整算法超参数，获取最佳模型
     *
     * @param dataframe 数据集
     * @return
     */
    def trainBestModel(dataframe: DataFrame): KMeansModel = {

        // TODO：模型调优方式二：调整算法超参数 -> MaxIter 最大迭代次数, 使用训练验证模式完成
        // 1. 设置MaxIter最大迭代次数：5， 10， 20
        val maxIters: Array[Int] = Array(5, 10, 15)
        // 2. 针对每个MaxIter训练模型
        val models: Array[(Double, Int, KMeansModel)] = maxIters.map { maxIter =>
            // i. 构建KMeans算法对象，设置相关参数
            val kMeans: KMeans = new KMeans()
                // 设置输入列和预测之列名称
                .setFeaturesCol("features")
                .setPredictionCol("prediction")
                .setK(5)
                .setMaxIter(maxIter)
                .setInitMode("k-means||")
            // ii. 使用数据集训练模型
            val model: KMeansModel = kMeans.fit(dataframe)
            // iii. 计算误差平方和SSE
            val wssse: Double = model.computeCost(dataframe)
            println(s"MaxIter = $maxIter, WSSSE = $wssse")
            // iv. 返回三元组：maxIter, wssse, model
            (wssse, maxIter, model)
        }
        // 3. 从训练模型中，获取最佳模型：wssse最小
        val (_, _, bestModel): (Double, Int, KMeansModel) = models.minBy(tuple => tuple._1)
        // 4. 返回最佳模型
        bestModel

    }

    /**
     * 加载模型，如果模型不存在，使用算法训练模型
     * @param dataframe 训练数据集
     * @return KMeansModel 模型
     */
    def loadModel(dataframe: DataFrame): KMeansModel = {
        // 1. 模型保存路径
        val modelPath: String = ModelConfig.MODEL_BASE_PATH + s"/${this.getClass.getSimpleName}"
        //获取hadoop信息
        val conf: Configuration = dataframe.sparkSession.sparkContext.hadoopConfiguration
        // 2.判断路径是否存在
        val model: KMeansModel = if (HdfsUtils.exists(conf, modelPath)) {
            logWarning(s"此时正在从【$modelPath】加载模型...................")
            KMeansModel.load(modelPath)
        } else {
            // 模型不存在，需要训练模型, 获取最佳模型
            logWarning(s"此时正在训练模型，获取最佳模型...................")
            val bestModel: KMeansModel = trainBestModel(dataframe)
            logWarning(s"此时正在保存模型至【$modelPath】...................")
            bestModel.save(modelPath)
            // 返回最佳模型
            bestModel
        }
        // 3. 返回模型
        model
    }

}


object RfmTagModel {
    def main(args: Array[String]): Unit = {
        val tagModel = new RfmTagModel()
        tagModel.executeModel(358L)
    }
}

