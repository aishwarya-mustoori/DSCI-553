import java.io.{BufferedWriter, File, FileWriter}
import java.util.Date

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

import scala.math.Ordering.Implicits.seqDerivedOrdering
import scala.util.control.Breaks


object task2 {
  def aprioriAlgo(partition: List[List[String]],count :Long, support: Int) : Iterator[List[String]] = {
    var i = 0
    var dictionaryForC1 =  Map[String, Long]()
    for (basket<-partition) {
      for(item<-basket){
        if(!dictionaryForC1.contains(item)){
          dictionaryForC1 += item->1
        }else{
          val count = dictionaryForC1(item) + 1
          dictionaryForC1 +=item-> count
        }
      }
      i = i + 1
    }
    val changed_support = Math.ceil(support*(i.toFloat/count))
    var l1 = List[List[String]]()
    for (i<-dictionaryForC1) {
      if(i._2 >= changed_support.toInt){
        l1 = l1 ::: List(List(i._1))
      }
    }
    l1 = l1.sortBy(x=>x(0))
    var ans = l1
    // For all other pairs
    var min_k = 2
    var frequent_itemsets = l1
    var flag = true
    while(flag){
      var Cn= List[List[String]]()
      for (candidate <- frequent_itemsets.toSeq.combinations(2)) {
        val first = candidate(0)
        val second = candidate(1)
        val candList = (first ::: second).toSet.toList.sorted
        val loop = new Breaks
        if(min_k== candList.length && !Cn.contains(candList)){
          var main_count = 0
          loop.breakable {
            for (basket<-partition) {
              if (candList.toSet.subsetOf(basket.toSet)) {
                main_count = main_count + 1
                if (main_count >= changed_support) {
                  Cn = Cn ::: List(candList)
                  loop.break()
                }
              }
            }
          }

        }

      }

      if(Cn.length==0) {
        val loop = new Breaks
        loop.breakable {
          flag = false
          loop.break()
        }

      }else{
        Cn = Cn.sorted
        ans = ans ::: Cn.toSet.toList
        min_k = min_k+1
        frequent_itemsets= Cn
      }

    }
    ans.iterator
  }
  def mapPhase2(partition: List[List[String]],candidatePairs: List[List[String]]) : Iterator[(List[String],Long)] = {

    var ans = List[(List[String],Long)]()
    for (pair <- candidatePairs) {
      var main_count = 0
      for (basket <- partition) {
        if (pair.toSet.subsetOf(basket.toSet)) {
          main_count = main_count + 1
        }
      }
      val tuple = pair -> main_count.toLong
      ans = ans ::: List(tuple)

    }
    ans.iterator
  }

  def main(args: Array[String]): Unit = {
    val start = new Date().getTime
    val conf = new SparkConf().setAppName("spark-test").setMaster("local[*]")
    val sc = new SparkContext(conf)
    val support = args(0).trim().toInt
    val textFile = sc.textFile(args(1).trim())
    val header = textFile.take(1)
    val filter = textFile.filter(x=>x!=header.toList(0))
    val row = filter.map(row => row.split(","))

    val main = row.map(x => (x(0), x(1))).reduceByKey((x, y) => (x + "," + y))
    val count = main.count()
    val candidatesPairs = main.map(x=>x._2.split(",").toList).mapPartitions(x=>aprioriAlgo(x.toList,count,support)).distinct().sortBy(x=>(x.length,x))
    val collect = candidatesPairs.collect()
    val frequentPairs = main.map(x=>x._2.split(",").toList).mapPartitions(x=>mapPhase2(x.toList,collect.toList)).reduceByKey((x,y)=>x+y).filter(x=>x._2>=support.toLong).distinct().sortBy(x=>(x._1.length,x._1))
    val frequent = frequentPairs.collect().toList
    val canidates = candidatesPairs.collect().toList
    val file = new File(args(2).trim())
    val bw = new BufferedWriter(new FileWriter(file))
    var index = 0
    bw.write("Candidates:" )
    for (pairs <- canidates) {
      if(index == pairs.length) {
        bw.write(",")
        bw.write("(")
        for (value<- pairs){
          bw.write("'"+value +"'")
          if(value != pairs.last){
            bw.write(", " )
          }
        }

        bw.write(")")
      }
      else{
        if(index == 0){
          bw.write("\n")
        }else{
          bw.write("\n")
          bw.write("\n")
        }
        index = index + 1
        bw.write("(")
        for (value<- pairs){
          bw.write("'"+value+"'" )
          if(value != pairs.last){
            bw.write(", " )
          }
        }
        bw.write(")")
      }
    }
    index = 0
    bw.write("\n\nFrequent Itemsets:" )
    for (pairs <- frequent) {
      if(index == pairs._1.length) {
        bw.write(",")
        bw.write("(")
        for (value<- pairs._1){
          bw.write("'"+value +"'")
          if(value != pairs._1.last){
            bw.write(", " )
          }
        }

        bw.write(")")
      }
      else{
        if(index == 0){
          bw.write("\n")
        }else{
          bw.write("\n")
          bw.write("\n")
        }
        index = index + 1
        bw.write("(")
        for (value<- pairs._1){
          bw.write("'"+value+"'" )
          if(value != pairs._1.last){
            bw.write(", " )
          }
        }
        bw.write(")")
      }
    }
    bw.close()
    sc.stop()
    val end = new Date().getTime
    println("Duration: "+((end-start)/1000.0).toFloat)
  }
}

