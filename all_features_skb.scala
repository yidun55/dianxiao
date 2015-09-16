import org.apache.spark._
import org.apache.spark.rdd.RDD 
import scala.collection.mutable.Set
import org.apache.spark.mllib.tree.RandomForest
import org.apache.spark.mllib.regression.LabeledPoint 
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.mllib.linalg.Vector 
import org.apache.spark.mllib.util.MLUtils
import Array._
import scala.collection.mutable.ArrayBuffer
import org.apache.spark.storage.StorageLevel

/*
dengyouhui
*/

object DRandomForest extends App{
    val sparkConf = new SparkConf().setAppName("pred_source_all")
    val sc = new SparkContext(sparkConf)
//===========================================================
//function for data transform
    def numerical(in_path:String, col_num:Int)={
        /*
        get the mean of the column containing na
        */
        val in = scala.io.Source.fromFile(new java.io.File(in_path)).getLines()
        var tmp = ArrayBuffer[String]()
        for(line<-in){
            val fields = line.split("\001")
            tmp += fields(col_num)
          //println(fields(97))
        }
        var contain = ArrayBuffer[Double]()
        for(i <- tmp){
            try{
                contain += i.toDouble
            }catch{
                case e:Exception=> println("na")
            }
        }
        contain.sum/contain.length.toDouble
    }

    def classi(in_path:String,col_num:Int)={
        /*
        tranform features from char to numerical
        */
        val in = scala.io.Source.fromFile(new java.io.File(in_path)).getLines()
        var tmp = ArrayBuffer[String]()
        for(line<-in){
            val fields = line.split("\001")
            tmp += fields(col_num)
          //println(fields(97))
        }
        var uni = tmp.toSet
        var zi = uni.zipWithIndex
        var zi_map = zi.toMap
        zi_map     
    }    

//======================================================
// load data and transform data
    path = "/data/mllib/test_source_all_clean"
    var class_f = range(80, 83).toBuffer
    class_f += (84)
    class_f ++= range(87, 91)
    var num_f = Array(91, 97)
    all_map = Map(87->Map("长沙"->1), ......)
    var all_map = ArrayBuffer[any]()
    for(col<-class_f){
        all_map += classi(path,col)
    }
    for(col<-num_f){
        all_map += numerical(path, col)
    }
    var col_all = class_f.toBuffer
    col_all ++= num_f
    var all_map_zip = (all_map zip col_all).toMap //Map(87->Map("长沙"->1), ......)

    var data_p = sc.textFile("/data/mllib/test_source_all_clean").map{
        line => 
        val fields = line.split("\001")
        val all_col = range(1, fields.length).toBuffer
        var vfields = new ArrayBuffer[Double]()
        for(col <- all_col){
            if(class_f.exists(el=>el==col)){
                try{
                    vfields += all_map(col)(fields(col)).toDouble
                }catch{
                    case e:Exception=> vfields(col)=0L
                }
            }else if(num_f.exists(el=>el==col)){
                try{
                    if(fields(col)=="NA"){
                        vfields += all_map(col).toDouble
                    }else{
                        vfields += fields(col).toDouble
                    }catch{
                        case e:Exception=>println("error")
                    }
                }
            }else{
                try{
                    vfields += fields(col).toDouble
                }catch{
                    case e:Exception=> vfields(col)=0L
                }                
            }
        }
        //if(fields(0).toDouble==1){
            var label = fields(0).toDouble
            var indices = range(0, vfields.length)
            var value = vfields.toArray
            (label, indices, value)
        //}
    }





}