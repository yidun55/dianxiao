import org.apache.spark._
import org.apache.spark.rdd.RDD 
import scala.collection.mutable.Set
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.regression.LabeledPoint 
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector 
import org.apache.spark.mllib.util.MLUtils
import Array._

/*
dengyouhui
*/
object DRandomForest extends App{
    val sparkConf = new SparkConf().setAppName("pred_source_all")
    val sc = new SparkContext(sparkConf)
//===========================================================
//load Data set for training model
    var del_col = range(80, 83).toBuffer
    del_col += (84)
    del_col ++= range(87, 92)
    del_col += (97)  //the columns to be removed

    //load data which from users buy products
    var data_p: RDD[LabeledPoint] = sc.textFile("/data/mllib/test_source_all_clean").map{
        line => 
        val fields = line.split("\001")
        val exist_col = range(1, fields.length).toBuffer
                               .filter(s=>del_col.exists(j=>j!=s))
        var vfields = new Array[Double](exist_col.length)
        for(col <- exist_col){
            if(fields(0)==1){
                try{
                    vfields += fields(col).toDouble
                }catch{
                    case e:Exception=> vfields(i)=0L
                }
            }  
        }
        LabeledPoint(fields(0).toDouble, Vectors.dense(vfields))
    }

    var data_0827:RDD[LabeledPoint] = sc.textFile("/data/mllib/skb_test_0827").map{
        line =>
        val fields = line.split("\001")
        val exist_col = range(1, fields.length-2).toBuffer  //rm last to col
                               .filter(s=>del_col.exists(j=>j!=s))
        var vfields = new Array[Double](exist_col.length)
        for(col <- exist_col){
            try{
                vfields += fields(col).toDouble
            }catch{
                case e:Exception=> vfields(i)=0L
            }
        }
        LabeledPoint(fields(fields.length-1).toDouble, Vectors.dense(vfields))
        //The last col is response variable
    }
    data_union = data_0827.union(data_p.sample(true, 1.5))
    //balance data set
//===================================================================
// load data set for prediction
var to_predict:RDD[LabeledPoint] = sc.textFile("/data/mllib/pred_source_all_20150905").map{
    line => 
    val fields = line.split("\001")
    val exist_col = range(1, fields.length).toBuffer
                               .filter(s=>del_col.exists(j=>j!=s))
    var vfields = new Array[Double](exist_col.length)
        for(col <- exist_col){
            try{
                vfields += fields(col).toDouble
            }catch{
                case e:Exception=> vfields(i)=0L
            } 
        }
        LabeledPoint(fields(0).toDouble, Vectors.dense(vfields))
}

//===================================================================


    //split data set
    var splits = data.randomSplit(Array(0.7, 0.3))
    var (trainData, testData) = (splits(0), splits(1))

    //train model
    var numClasses = 2
    var categoricalFeaturesInfo = Map[Int, Int]()
    var numTrees = 100
    var featureSubsetStrategy = "auto"
    var impurity = "gini"
    var maxDepth = 6
    var maxBins = 32

    var model = RandomForest.trainClassifier(trainData, numClasses,
        categoricalFeaturesInfo, numTrees, featureSubsetStrategy,
        impurity, maxDepth, maxBins)

    //Evaluate model
    var labelAndPreds = testData.map{
        point =>
        val pred = model.predict(point.features)
        (point.label, pred)
    }
    var testErr = labelAndPreds.filter(r => r._1 != r._2).count.toDouble/testData.count()
    println("Test Error = " + testErr)
//==========================================================
//predict and save the positive data
    var mobileAndPreds = to_predict.map{
        point =>
        val pred = model.predict(point.features)
        (point.label, pred)   //point.label = mobile num
    }
    var saved_data = mobileAndPreds.filter(r=>r._2==1).repartition(1)
         .saveAsTextFile("dyh_output.txt")

}