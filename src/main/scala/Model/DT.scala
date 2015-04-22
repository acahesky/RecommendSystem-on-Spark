package model

import java.sql.Timestamp

import base._
import model.FeatureGenerator
import org.apache.spark.mllib.classification.{LogisticRegressionModel, LogisticRegressionWithLBFGS}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.mllib.tree.{DecisionTree, RandomForest, GradientBoostedTrees}
import org.apache.spark.mllib.tree.configuration.BoostingStrategy
import org.apache.spark.mllib.tree.model.GradientBoostedTreesModel
import org.apache.spark.mllib.util.MLUtils
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}
object DT {

  val pc = 0.04
  val modelName = "GBDT"
  def runModel(modelName:String,trainData:RDD[LabeledPoint],testData:RDD[(UISet,LabeledPoint)])={
    val numClasses = 2
    val categoricalFeaturesInfo = Map[Int, Int]()
    val numTrees = 100 // Use more in practice.
    val featureSubsetStrategy = "auto" // Let the algorithm choose.
    val impurity = "gini"
//    val maxDepth = 4
//    val maxBins = 32
    val maxDepth = 30
    val maxBins = 50

    modelName match {
      case "LR" => {
        println("LR")
        val model = new LogisticRegressionWithLBFGS()
          .setNumClasses(2)
          .run(trainData)
        testData.map { point =>
          val predict = model.predict(point._2.features)
          (point._1, predict)
        }
      }
      case "DecisionTree" => {
        println("DT")
        val categoricalFeaturesInfo = Map[Int, Int]()
        val model = DecisionTree.trainClassifier(trainData,numClasses,categoricalFeaturesInfo,impurity,maxDepth,maxBins)
        testData.map { point =>
          val predict = model.predict(point._2.features)
          (point._1, predict)
      }
    }
      case "GBDT" => {
        println("GB")
        import org.apache.spark.mllib.tree.configuration._
          val boostingStrategy = BoostingStrategy.defaultParams("Classification")
          boostingStrategy.numIterations = 10 // Note: Use more iterations in practice.
          boostingStrategy.treeStrategy.numClasses = 2
          boostingStrategy.treeStrategy.maxDepth = 20
          //  Empty categoricalFeaturesInfo indicates all features are continuous.
          boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()
          val model = GradientBoostedTrees.train(trainData, boostingStrategy)
        testData.map { point =>
          val predict = model.predict(point._2.features)
          (point._1, predict)
        }
      }
      case "RF" => {
        println("RF")
        val model = RandomForest.trainClassifier(trainData, numClasses, categoricalFeaturesInfo,
          numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins)
        testData.map { point =>
          val predict = model.predict(point._2.features)
          (point._1, predict)
        }
      }
    }
  }
  def getDTResult(positive:RDD[LabeledPoint],negativeAll:RDD[LabeledPoint],testData:RDD[(UISet,LabeledPoint)]) = {

    val negative = negativeAll.randomSplit(Array(pc,1-pc)).apply(0)
    println("positive: " +positive.count())
    println("negative: "+ negative.count())
    val trainingDataBlock = negative.union(positive)
    val prediction = runModel(modelName,trainingDataBlock,testData)
    prediction.filter(x =>x._2 == 1).map(x => x._1)

  }
  def getUserDTResult(positive:RDD[LabeledPoint],negativeAll:RDD[LabeledPoint],testData:RDD[(Int,LabeledPoint)]) = {

    val negative = negativeAll.randomSplit(Array(pc,1-pc)).apply(0)
    println("positive: " +positive.count())
    println("negative: "+ negative.count())
    val trainData = negative.union(positive)
    import org.apache.spark.mllib.tree.configuration._
    val boostingStrategy = BoostingStrategy.defaultParams("Classification")

    boostingStrategy.numIterations = 10 // Note: Use more iterations in practice.
    boostingStrategy.treeStrategy.numClasses = 2
    boostingStrategy.treeStrategy.maxDepth = 20
    boostingStrategy.treeStrategy.categoricalFeaturesInfo = Map[Int, Int]()

    val model = GradientBoostedTrees.train(trainData, boostingStrategy)
    val prediction = testData.map { point =>
      val predict = model.predict(point._2.features)
      (point._1, predict)
    }
    prediction.filter(x =>x._2 == 1).map(x => x._1)
  }

  def getLabel(rdd: RDD[UIData]) = {
    rdd.filter(x => x.behaviorType == 4).map(x => UI(x.userId,x.itemId)).distinct()
  }
  def getUserLabel(rdd: RDD[UIData]) = {
    rdd.filter(x => x.behaviorType == 4).map(x => x.userId).distinct()
  }
  def getFullLabeledPoint(rdd:RDD[UIData],startTime:Timestamp,endTime:Timestamp,preTime:Timestamp)={

    val adapter = DataSpliter.splitDatabyMultiDate(rdd,Array((startTime,endTime),(endTime,preTime)))
    val labelSet = getLabel(adapter(1)).collect()
    FeatureGenerator
      .getDTFeatureWithUISet(FeatureGenerator.getFeature(adapter(0),endTime),labelSet)
      .map(x => LabeledPoint(x._2.label,x._2.toDense))
  }
  def getUserLabledPoint(rdd:RDD[UIData],startTime:Timestamp,endTime:Timestamp,preTime:Timestamp) = {
    val adapter = DataSpliter.splitDatabyMultiDate(rdd,Array((startTime,endTime),(endTime,preTime)))
    val labelSet = getUserLabel(adapter(1)).collect()
    FeatureGenerator
      .getUserDTFeatureWithUISet(FeatureGenerator.getUserFeature(adapter(0),endTime),labelSet)
      .map(x => LabeledPoint(x._2._1,x._2._2))
  }
  def getUserLabledPointWithUI(rdd:RDD[UIData],startTime:Timestamp,endTime:Timestamp,preTime:Timestamp) = {
    val adapter = DataSpliter.splitDatabyMultiDate(rdd,Array((startTime,endTime),(endTime,preTime)))
    val labelSet = getUserLabel(adapter(1)).collect()
    FeatureGenerator
      .getUserDTFeatureWithUISet(FeatureGenerator.getUserFeature(adapter(0),endTime),labelSet)
      .map(x => (x._1,LabeledPoint(x._2._1,x._2._2)))
  }
  def getFullLabeledPointWithUI(rdd:RDD[UIData],startTime:Timestamp,endTime:Timestamp) = {

    val adapter = DataSpliter.splitDatabyMultiDate(rdd,Array((startTime,endTime)))
//    val labelSet = getLabel(adapter(1)).collect()
    FeatureGenerator
      .getDTFeatureWithUISet(FeatureGenerator.getFeature(adapter(0),endTime),Array[UI]())
      .map(x => (x._1,LabeledPoint(x._2.label,x._2.toDense)))
  }
  def getDate(month:Int,day :Int) = {
    val oc1th = Timestamp.valueOf("2014-11-1 0:0:0")
    val oneDay = Timestamp.valueOf("2014-11-18 0:0:0").getTime - Timestamp.valueOf("2014-11-17 0:0:0").getTime
    new Timestamp(((if(month == 12 ) 29 else -1)+day) * oneDay+oc1th.getTime)
  }
}
