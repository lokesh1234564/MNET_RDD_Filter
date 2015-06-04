import org.apache.spark.SparkContext._
import java.security.MessageDigest
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import java.security.MessageDigest
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
import org.apache.spark.rdd._
import scala.math
import java.io._

import com.esotericsoftware.kryo.Kryo
import org.apache.spark.serializer.KryoRegistrator


class DSV (var line:String="", var delimiter:String=",",var parts:Array[String]=Array("")) extends Serializable {

         parts=line.split(delimiter,-1)

def hasValidVal(index: Int):Boolean={
    return (parts(index)!=null)&&(parts.length>index)
}
def contains(text:String):Boolean={

    for(i <- 1 to (parts.length-1))
        if(parts(i).contains(text))
            return false

    true
}

}
class MyRegistrator extends KryoRegistrator {
  override def registerClasses(kryo: Kryo) {
//    kryo.register(classOf[Location])
//    kryo.register(classOf[ModalCOG])
    kryo.register(classOf[DSV])
  }
}


object RDD_Filter extends Serializable{
                val conf = new SparkConf().setMaster("yarn-client")
                //setMaster("spark://messi.ischool.uw.edu:7077")
                .setAppName("MigrationDegreeAnalysis_4Months")
                .set("spark.shuffle.consolidateFiles", "true")
                .set("spark.storage.blockManagerHeartBeatMs", "300000")
                .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
                .set("spark.kryo.registrator", "MyRegistrator")
                .set("spark.akka.frameSize","1024")
                .set("spark.default.parallelism","200")
                //.set("spark.executor.memory", "40g")
                .set("spark.kryoserializer.buffer.max.mb","10024")
                .set("spark.kryoserializer.buffer.mb","1024")

                val sc = new SparkContext(conf)

        	//val inputPath="hdfs:///user/mraza/Rwanda_In/CallsVolDegree/"
        	//val outputPath = "hdfs:///user/mraza/Rwanda_Out/DistrictDegreeMigration_4months/"

		val inputPath="Rwanda_In/CallsVolDegree/"
        	val outputPath = "Rwanda_Out/CallsVolDegree/"


def getMeanStd(rdd:RDD[(String, String)]):(Double, Double, Double)={
	val scores:RDD[Double]=rdd.map{case(k,v)=>(v.toDouble)}.cache
	val count=scores.count
	val mean=scores.mean
	//val devs = scores.map(score => (score - mean) * (score - mean))
	val std = scores.stdev
	
	val percentile_99th_value=scores.top((count/100).toInt).last
	
	return (mean, std, percentile_99th_value)	
}  


def main(args:Array[String]){


        var month=args(0)

        var callDegreeFile1=args(1)
        
	//var callDegreeFile1 = "0604-ModalCallVolDegree.csv"
	//SubscriberId,Month,A-District,A-Province,B-District,B-Province,A-Volume,A-Degree,A-TotalVolume,A-TotalDegree
	//L72656815,0501,Bugesera,East,Bugesera,East,8,4,8,4

	//Extract the subscriber, total degree and total volume from the RDD
	
	var full_rdd= sc.textFile(inputPath+callDegreeFile1,10).filter(d=>d.contains("Degree")==false).map(line=>line.replaceAll("\\)","").replaceAll("\\(","")).map(line=>(new DSV(line,"\\,"))).map(d=>(d.parts(0),(d.parts(1),d.parts(2),d.parts(3),d.parts(4),d.parts(5),d.parts(6),d.parts(7),d.parts(8),d.parts(9))))

	var rdd = full_rdd.map{case(k,v)=>(k,(v._8,v._9))}.distinct()

	rdd.count()

	var rdd_degree =rdd.map{case(k,v)=>(k,(v._2))}

	var logData:String="RDD_Degree count"+rdd_degree.count()


	var rdd_volume = rdd.map{case(k,v)=>(k,(v._1))}

	logData+="RDD+Volume count"+rdd_volume.count()

	

	var (mean_degree, stdev_degree, percentile_99th_degree) = getMeanStd(rdd_degree)

	logData+="Mean_Degree, stdev_degree, percentile_99th_degree"+mean_degree+" ,"+stdev_degree+", "+percentile_99th_degree+"\n"

	var (mean_volume, stdev_volume, percentile_99th_volume) = getMeanStd(rdd_volume)

	logData+="Mean_Volume, stdev_Volume, percentile_99th_volume"+mean_volume+" ,"+stdev_volume+", "+percentile_99th_volume+"\n"
	
	var 	rdd_degree_filtered  = rdd_degree.filter{case(k,v)=>(v.toDouble<percentile_99th_degree)}
	
//	rdd_degree_filtered.count()

	logData+="Rdd_degree_filtered_count"+rdd_degree_filtered.count()

	var rdd_volume_filtered = rdd_volume.filter{case(k,v)=>(v.toDouble<percentile_99th_volume)}

	logData+="Rdd_degree_filtered_count"+rdd_volume_filtered.count()
	//rdd_volume_filtered.count()

	var rdd_filtered = rdd_degree_filtered.join(rdd_volume_filtered)

	rdd_filtered.count()	

	var rdd_filtered_final=full_rdd.join(rdd_filtered).map{case(k,v)=>(k,(v._1))}

	rdd_filtered_final.count()

	rdd_filtered_final.saveAsTextFile(outputPath+"filtered"+callDegreeFile1)
	sc.parallelize(logData).coalesce(1).saveAsTextFile(outputPath+month+"filteredlog.txt")
}

}
