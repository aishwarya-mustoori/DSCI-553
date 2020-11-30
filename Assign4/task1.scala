import java.io.{BufferedWriter, File, FileWriter}

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.types.StructType
import org.graphframes.GraphFrame
import org.json4s.JsonAST.JString

import scala.math.Ordering.Implicits.seqDerivedOrdering


object task1 {

    def main(args: Array[String]): Unit = {
      val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
      val sc = new SparkContext(conf)
      val sqlContext = new SQLContext(sc)
      sc.setLogLevel("ERROR")
      val threshold = args(0).trim().toInt
      val file = args(2)
      val textFile = sc.textFile(args(1).trim()).map(x => x.split(","))
      val header = textFile.take(1)
      val filter = textFile.filter(x => x != header.toList(0))
      val sample_text = filter.map(x => (x(1), x(0))).reduceByKey((x,y)=>x+" "+y).map(x=>(x._1,x._2.split(" ")))
      val edges = sample_text.flatMap(x =>( for(y <-x._2.permutations) yield (Tuple1(y), x._1)  ))
        //.reduceByKey((x,y) => x + " " + y).filter(x =>  x._2.split(" ").toSet.toList.length >= threshold).map(x => (x._1)).distinct()
      println(edges.take(1).toList)
      println("yes")
            val edgesDF = sqlContext.createDataFrame(edges.collect()).toDF("src","dst")

            val vertices = edges.map( x=> Tuple1(x._1)).distinct()

            val verticesDF =sqlContext.createDataFrame(vertices).toDF("id")

            val g = GraphFrame( verticesDF, edgesDF)

            val comm = g.labelPropagation.maxIter(5).run()

            val comm_rdd = comm.select("label","id").rdd.map( x=>(JString(x(0).toString),x(1).toString)).reduceByKey((x, y)=>x+", "+y).map(x=>(x._1,x._2.split(", ").sorted)).sortBy( x =>(x._2.toList.length,x._2.toList.sorted)).collect()

            val bw = new BufferedWriter(new FileWriter(file))

              for ( community <-comm_rdd ) {
                var i = 0
                for (node<- community._2){
                  if(i==0) {
                    bw.write("'"+node+"'")
                    i = i + 1

                  }else{
                    bw.write(", '"+node+"'")
                  }
                }

            bw.write("\n")
              }
            bw.close()

        }

}

