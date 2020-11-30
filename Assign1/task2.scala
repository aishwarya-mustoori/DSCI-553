object task2_1{
        def main(args:Array[String]):Unit={
        if(args(3)=="spark"){
        val conf=new SparkConf().setAppName("spark-test").setMaster("local[*]")
        val sc=new SparkContext(conf)
        val textFile=sc.textFile(args(0))
        val review_json=textFile.map(row=>parse(row))
        val review_row=review_json.map(x=>((x \ "business_id").values.toString,(x \ "stars").values.toString))

        val business_data=sc.textFile(args(1))
        val business_j=business_data.map(x=>parse(x))

        val business_json=business_j.filter(x=>(x \ "categories")!=JNull).flatMap(x=>for(n<-(x \\ "categories").values.toString.split(','))yield{((x \\ "business_id").values.toString,n.trim())})
        val join1=review_row.join(business_json)
        val reduce=join1.map(x=>(x._2._2,(x._2._1,1))).reduceByKey((x,y)=>((x._1.toDouble+y._1.toDouble).toString,y._2.toInt+x._2.toInt)).map(x=>(x._1,(x._2._1.toDouble/x._2._2).toDouble)).sortBy(x=>(-x._2,x._1)).map(x=>List(x._1,x._2))

        val ans="result"->reduce.take(args(4).toInt).toList
        val file=new File(args(2))
        val bw=new BufferedWriter(new FileWriter(file))

        bw.write(Json(DefaultFormats).write(ans).replace(":",": ").replace(",",", "))
        bw.close()
        sc.stop()


        }else{

        //NOSPARK IMPLEMENTATION
        var business_map=Map[String,List[String]]()
        val textFile=Source.fromFile(args(1))
        for(i<-textFile.getLines){
        val line=parse(i)
        if((line \ "categories")!=JNull&&(line \ "categories")!=""){
        for(category<-(line \ "categories").values.toString.split(", ")){
        val business_id=(line \ "business_id").values.toString
        if(!business_map.contains(category)){
        business_map+=category->List(business_id)
        }else{
        val main=business_map(category):::List(business_id)
        business_map+=category->main
        }

        }
        }
        }
        var review_map=Map[String,List[Any]]()
        val review_file=Source.fromFile(args(0))
        for(i<-review_file.getLines){
        val line=parse(i)
        val business_id=(line \ "business_id").values.toString
        val stars=(line \ "stars").values.toString.toDouble
        if(!review_map.contains(business_id)){
        review_map+=business_id->List(stars,1.0)
        }else{
        val main=review_map(business_id)
        val newValue=List(main(0).toString.toDouble+stars,main(1).toString.toDouble+1.0)
        review_map+=business_id->newValue
        }

        }
        var ans_map=List[List[Any]]()
        business_map.keys.foreach{i=>
        var sum=0.0
        var count=0.0
        for(ids<-business_map(i)){
        if(review_map.contains(ids)){
        sum=sum+review_map(ids)(0).toString.toDouble
        count=count+review_map(ids)(1).toString.toDouble
        }
        }
        if(count!=0.0){
        ans_map=ans_map:::List(List(i,(sum/count)))
        }
        }
        val ans=ans_map.sortBy(x=>(x(1).toString.toDouble,x(0).toString))(Ordering.Tuple2(Ordering.Double.reverse,Ordering.String)).slice(0,args(4).toInt)
        val file=new File(args(2))
        val bw=new BufferedWriter(new FileWriter(file))

        bw.write(Json(DefaultFormats).write(ans).replace(":",": ").replace(",",", "))
        bw.close()

        }

        }
        }
