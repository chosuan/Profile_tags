package cn.itcast.tags.models.ml

import cn.itcast.tags.config.ModelConfig
import cn.itcast.tags.models.{AbstractModel, ModelType}
import cn.itcast.tags.tools.TagTools
import cn.itcast.tags.utils.HdfsUtils
import org.apache.hadoop.conf.Configuration
import org.apache.spark.ml.clustering.{KMeans, KMeansModel}
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.ml.linalg
import org.apache.spark.sql.{Column, DataFrame}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{DataTypes, DecimalType}

/**
 * 挖掘类型标签模型开发：用户客户价值标签模型（RFM模型）
 */
class RfmModelTest extends AbstractModel("决策类型RFM标签", ModelType.ML) {
    /*
    358	客户价值
      359	  高价值		      0
      360	  中上价值	    	1
      361	  中价值		      2
      362	  中下价值	    	3
      363	  超低价值	    	4
     */
    override def doTag(businessDF: DataFrame, tagDF: DataFrame): DataFrame = {

        import businessDF.sparkSession.implicits._
        /*
            root
             |-- memberid: string (nullable = true)
             |-- ordersn: string (nullable = true)
             |-- orderamount: string (nullable = true)
             |-- finishtime: string (nullable = true)
         */
        //businessDF.printSchema()
        //businessDF.show(20,false)
        /*
            root
             |-- id: long (nullable = false)
             |-- name: string (nullable = true)
             |-- rule: string (nullable = true)
             |-- level: integer (nullable = true)
         */
        //tagDF.printSchema()
        //tagDF.show(20,false)

        val rfmDF: DataFrame = businessDF
            .groupBy($"memberid")
            .agg(
                max($"finishtime").as("finish_time"),
                count($"ordersn").as("frequency"),
                sum($"orderamount".cast(DataTypes.createDecimalType(10, 2))).as("monetary")
            )
            .select(
                $"memberid".as("uid"),
                datediff(current_timestamp(), from_unixtime($"finish_time")).as("recency"),
                $"frequency", $"monetary"
            )

        //        rfmDF.printSchema()
        /*
            +---+-------+---------+---------+
            |uid|recency|frequency|monetary |
            +---+-------+---------+---------+
            |1  |78     |234      |552976.37|
            |102|78     |121      |199246.79|
            |107|78     |127      |185029.83|
            |110|78     |96       |175474.38|
         */
        //        rfmDF.show(20,false)

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
        val fWhen: Column = when(col("frequency").geq(200), 5.0)
            .when(column("frequency").between(150, 199), 4.0)
            .when(column("frequency").between(100, 149), 3.0)
            .when(column("frequency").between(50, 99), 2.0)
            .when(column("frequency").between(1, 49), 1.0)
        // M 打分条件表达式
        val mWhen = when(col("monetary").lt(10000), 1.0) //
            .when(col("monetary").between(10000, 49999), 2.0) //
            .when(col("monetary").between(50000, 99999), 3.0) //
            .when(col("monetary").between(100000, 199999), 4.0) //
            .when(col("monetary").geq(200000), 5.0) //

        val rfmScoreDF: DataFrame = rfmDF.select(
            $"uid",
            rWhen.as("r_score"),
            fWhen.as("f_score"),
            mWhen.as("m_score")
        )

        /*
            root
             |-- uid: string (nullable = true)
             |-- r_score: double (nullable = true)
             |-- f_score: double (nullable = true)
             |-- m_score: double (nullable = true)
         */
        //rfmScoreDF.printSchema()
        /*
            +---+-------+-------+-------+
            |uid|r_score|f_score|m_score|
            +---+-------+-------+-------+
            |1  |1.0    |5.0    |5.0    |
            |102|1.0    |3.0    |4.0    |
         */
        //rfmScoreDF.show(20,false)

        //组和成特征向量
        val assembler: VectorAssembler = new VectorAssembler()
            .setInputCols(Array("r_score", "f_score", "m_score"))
            .setOutputCol("rfmFeatures")

        val rfmFeaturesDF: DataFrame = assembler.transform(rfmScoreDF)
        /*
        +---+-------+-------+-------+-------------+
        |uid|r_score|f_score|m_score|rfmFeatures  |
        +---+-------+-------+-------+-------------+
        |1  |1.0    |5.0    |5.0    |[1.0,5.0,5.0]|
        |102|1.0    |3.0    |4.0    |[1.0,3.0,4.0]|
        |107|1.0    |3.0    |4.0    |[1.0,3.0,4.0]|
         */
        //featuresDF.printSchema()
        //featuresDF.show(10,false)

        // 3.2 使用KMeans算法，训练模型
        //        val kMeansModel: KMeansModel = trainModel(rfmFeaturesDF)
        val kMeansModel: KMeansModel = trainBestModel(rfmFeaturesDF)


        val predictionDF: DataFrame = kMeansModel.transform(rfmFeaturesDF)
        //predictionDF.printSchema()
        /*
        +---+-------+-------+-------+-------------+----------+
        |uid|r_score|f_score|m_score|rfmFeatures  |prediction|
        +---+-------+-------+-------+-------------+----------+
        |1  |1.0    |5.0    |5.0    |[1.0,5.0,5.0]|2         |
        |102|1.0    |3.0    |4.0    |[1.0,3.0,4.0]|1         |
     */
        //predictionDF.show(10,false)
        //        val wssse: Double = kMeansModel.computeCost(rfmFeaturesDF)
        //        println(s"WSSSE =  ${wssse}") //   WSSSE =  0.99785407724886

        // 3.3 获取类簇中心点
        val clusterCenters: Array[linalg.Vector] = kMeansModel.clusterCenters
        val clusterIndexArray: Array[((Double, Int), Int)] = clusterCenters
            .zipWithIndex
            .map { case (vector, clusterCenter) => (vector.toArray.sum, clusterCenter) }
            .sortBy { case (rfm, _) => -rfm }
            .zipWithIndex
        /*
        todo Array[((Double, Int), Int)]
        sum                     clusterIndex      index
        ((11.0,                     2),             0)
        ((9.00214592274678,         0),             1)
        ((8.0,                      1),             2)
        ((8.0,                      4),             3)
        ((7.0,                      3),             4)
         */
        //            .foreach(println)

        val ruleMap: Map[String, Long] = TagTools.convertMap(tagDF)
        //        ruleMap.foreach(println)
        // 4.2 对类簇中心点数据进行遍历，获取对应tagId
        val clusterTagMap: Map[Int, Long] = clusterIndexArray.map { case ((_, clusterIndex), index) =>
            val tagId: Long = ruleMap(index.toString)
            (clusterIndex, tagId)
        }.toMap

        // 4.3 自定义UDF函数，传递prediction，返回tagId
        val index_to_tag = udf(
            (clusterIndex: Int) => {
                val tagId: Long = clusterTagMap(clusterIndex)
                tagId
            }
        )

        val modelDF: DataFrame = predictionDF.select($"uid",
            index_to_tag($"prediction").as("tagId")
        )
        //        modelDF.printSchema()
        //        modelDF.show(100, truncate = false)


        //        modelDF
        null
    }

    def trainModel(dataframe: DataFrame): KMeansModel = {
        val kMeans: KMeans = new KMeans()
            .setFeaturesCol("rfmFeatures")
            .setPredictionCol("prediction")
            .setK(5)
            .setMaxIter(20)
            .setInitMode("k-means||")
        val kMeansModel: KMeansModel = kMeans.fit(dataframe)
        val ssse: Double = kMeansModel.computeCost(dataframe)
        println(s"WSSSE = ${ssse}")
        kMeansModel
    }

    def trainBestModel(dataframe: DataFrame): KMeansModel = {

        val maxIters: Array[Int] = Array(5, 10, 15)
        val models: Array[(Double, Int, KMeansModel)] = maxIters.map { maxIter =>
            val kMeans: KMeans = new KMeans()
                .setFeaturesCol("rfmFeatures")
                .setPredictionCol("prediction")
                .setK(5)
                .setMaxIter(maxIter)
                .setInitMode("k-means||")
            val model: KMeansModel = kMeans.fit(dataframe)
            val ssse: Double = model.computeCost(dataframe)
            println(s"MaxIter = $maxIter, WSSSE = $ssse")
            println(s"WSSSE = ${ssse}")
            (ssse, maxIter, model)
        }
        val (_, _, bestModel: KMeansModel) = models.minBy(tuple => tuple._1)

        bestModel

    }

    def loadModel(dataFrame: DataFrame) : KMeansModel = {

        val modelPath = ModelConfig.MODEL_BASE_PATH + s"/${this.getClass.getSimpleName}"

        val conf: Configuration = dataFrame.sparkSession.sparkContext.hadoopConfiguration

        val model: KMeansModel = if(HdfsUtils.exists(conf , modelPath)){
            println(s"正在加载模型--路径为${modelPath}")
            val kMeansModel: KMeansModel = KMeansModel.load(modelPath)
            kMeansModel
        }else{
            println(s"不存在模型 正在训练---")
            val bestModel: KMeansModel = trainBestModel(dataFrame)
            println(s" 正在保存--- 路径为${modelPath}")
            bestModel.save(modelPath)
            bestModel
        }
        model

    }
}

object RfmModelTest {
    def main(args: Array[String]): Unit = {
        val rfmModelTest = new RfmModelTest
        rfmModelTest.executeModel(358L)
    }
}

