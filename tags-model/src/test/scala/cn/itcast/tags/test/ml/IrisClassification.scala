package cn.itcast.tags.test.ml

import org.apache.spark.ml.classification.{LogisticRegression, LogisticRegressionModel}
import org.apache.spark.ml.feature.{Normalizer, StringIndexer, StringIndexerModel, VectorAssembler}
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}
import org.apache.spark.storage.StorageLevel

object IrisClassification {

    def main(args: Array[String]): Unit = {
        // 构建SparkSession实例对象，通过建造者模式创建
        val spark: SparkSession = {
            SparkSession
                .builder()
                .appName(this.getClass.getSimpleName.stripSuffix("$"))
                .master("local[3]")
                .config("spark.sql.shuffle.partitions", "3")
                .getOrCreate()
        }

        // For implicit conversions like converting RDDs to DataFrames
        import spark.implicits._
        import org.apache.spark.sql.functions._

        // 自定义Schema信息
        val irisSchema: StructType = StructType(
            Array(
                StructField("sepal_length", DoubleType, nullable = true),
                StructField("sepal_width", DoubleType, nullable = true),
                StructField("petal_length", DoubleType, nullable = true),
                StructField("petal_width", DoubleType, nullable = true),
                StructField("category", StringType, nullable = true)
            )
        )
        // 1. 读取原始数据集：鸢尾花数据集，数据格式csv格式
        val rawIrisDF: DataFrame = spark.read
            .schema(irisSchema)
            .option("sep", ",")
            .option("encoding", "UTF-8")
            .option("header", "false")
            .option("inferSchema", "false")
            .csv("datas/iris/iris.data")
        //        rawIrisDF.printSchema()
        /*
        root
         |-- sepal_length: double (nullable = true)
         |-- sepal_width: double (nullable = true)
         |-- petal_length: double (nullable = true)
         |-- petal_width: double (nullable = true)
         |-- category: string (nullable = true)
         */
        //        rawIrisDF.show(10, truncate = false)

        // 2. 特征工程
        /*
                1、类别转换数值类型
                    类别特征索引化 -> label
                2、组合特征值
                    features: Vector
        */
        // 2.1 类别特征转换
        val indexerModel: StringIndexerModel = new StringIndexer()
            .setInputCol("category")
            .setOutputCol("label")
            .fit(rawIrisDF)
        val df1: DataFrame = indexerModel.transform(rawIrisDF)
        /*
            root
             |-- sepal_length: double (nullable = true)
             |-- sepal_width: double (nullable = true)
             |-- petal_length: double (nullable = true)
             |-- petal_width: double (nullable = true)
             |-- category: string (nullable = true)
             |-- label: double (nullable = true)
         */
        //        df1.printSchema()
        //        df1.show(150, truncate = false)

        // 2.2 组合特征值
        val assembler: VectorAssembler = new VectorAssembler()
            .setInputCols(rawIrisDF.columns.dropRight(1))
            .setOutputCol("raw_features")
        val df2: DataFrame = assembler.transform(df1)
        /*
            root
             |-- sepal_length: double (nullable = true)
             |-- sepal_width: double (nullable = true)
             |-- petal_length: double (nullable = true)
             |-- petal_width: double (nullable = true)
             |-- category: string (nullable = true)
             |-- label: double (nullable = true)
             |-- raw_features: vector (nullable = true)
         */
        df2.printSchema()
        df2.show(20, false)

        // 2.3 特征正则化   ---   L2正则化
        val normalizer: Normalizer = new Normalizer()
            .setInputCol("raw_features")
            .setOutputCol("features")
            .setP(2.0) //   L2正则化
        val featuresDF: DataFrame = normalizer.transform(df2)
        /*
            root
             |-- sepal_length: double (nullable = true)
             |-- sepal_width: double (nullable = true)
             |-- petal_length: double (nullable = true)
             |-- petal_width: double (nullable = true)
             |-- category: string (nullable = true)
             |-- label: double (nullable = true)
             |-- raw_features: vector (nullable = true)
             |-- features: vector (nullable = true)
         */
        //        featuresDF.printSchema()
        // 将数据集缓存，LR算法属于迭代算法，使用多次
        featuresDF.persist(StorageLevel.MEMORY_AND_DISK).count()

        // TODO: 将特征数据集划分为训练集和测试集
        val Array(trainingDF, testDF) = featuresDF.randomSplit(Array(0.9, 0.1), seed = 123L)
        // 由于使用逻辑回归分类算法，属于迭代算法，需要不停使用训练数据集训练模型，所以将训练缓存
        //trainingDF.persist(StorageLevel.MEMORY_AND_DISK).count()

        // 3. 使用特征数据应用到算法中训练模型
        val lr: LogisticRegression = new LogisticRegression()
            // 设置列名称
            .setLabelCol("label")
            .setFeaturesCol("features")
            .setPredictionCol("prediction")
            // 设置迭代次数
            .setMaxIter(10)
            .setRegParam(0.3) // 正则化参数
            .setElasticNetParam(0.8) // 弹性网络参数：L1正则和L2正则联合使用
        val lrModel: LogisticRegressionModel = lr.fit(featuresDF)

        // 4. 使用模型预测
        val predictionDF: DataFrame = lrModel.transform(featuresDF)
        predictionDF
         //获取真实标签类别和预测标签类别
            .select("label", "prediction")
            .show(150, false)

        // 5. 模型评估：使用测试数据集应用到模型中，获取预测值，与真实值进行比较
        // 模型评估：准确度 = 预测正确的样本数 / 所有的样本数
        import org.apache.spark.ml.evaluation.MulticlassClassificationEvaluator
        val evaluator: MulticlassClassificationEvaluator = new MulticlassClassificationEvaluator()
            .setLabelCol("label")
            .setPredictionCol("prediction")
            .setMetricName("accuracy")
        // ACCU = 0.4117647058823529
        println(s"ACCU = ${evaluator.evaluate(predictionDF)}")

        // 6. 模型调优，此处省略

        // 7. 模型保存与加载
        // TODO: 保存模型
        System.load("D:\\Program Files\\hadoop-2.6.0\\bin\\hadoop.dll")
        val modelPath = s"datas/models/lrModel-${System.nanoTime()}"
        lrModel.save(modelPath)

        // TODO: 加载模型
        val loadLrModel: LogisticRegressionModel = LogisticRegressionModel.load(modelPath)

        loadLrModel.transform(
            Seq(
                Vectors.dense(Array(5.1,3.5,1.4,0.2))
            )
                .map(x => Tuple1.apply(x))
                .toDF("features")
        ).show(1, false)

        spark.stop()

    }

}
