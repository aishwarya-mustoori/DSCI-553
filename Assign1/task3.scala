object task3_1{
        def main(args:Array[String]):Unit={
        val conf=new SparkConf().setAppName("spark-test").setMaster("local[*]")
        val sc=new SparkContext(conf)
        val textFile=sc.textFile(args(0).trim())

        val row=textFile.map(row=>parse(row))
        val n=args(4).trim().toInt
        val partion_no=args(3).trim().toInt
        var review_json=row.map(x=>((x \ "business_id").values.toString,1))
        if(args(2).trim()=="customized"){
        review_json=review_json.partitionBy(new HashPartitioner(partion_no)).persist()
        }

        val ans=Map(
        "n_partitions"->review_json.getNumPartitions,
        "n_items"->review_json.glom().map(x=>x.length).collect(),
        "result"->review_json.reduceByKey((x,y)=>x+y).filter(x=>x._2.toInt>n).map(x=>List(x._1,x._2)).collect())
        val file=new File(args(1).trim())
        val bw=new BufferedWriter(new FileWriter(file))

        bw.write(Json(DefaultFormats).write(ans).replace(":",": ").replace(",",", "))
        bw.close()
        sc.stop()

        }

        }
