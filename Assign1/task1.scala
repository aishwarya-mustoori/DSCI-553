object task1_1{
        def main(args:Array[String]):Unit={
        val conf=new SparkConf().setAppName("spark-test").setMaster("local[*]")
        val sc=new SparkContext(conf)
        val textFile=sc.textFile(args(0).trim())

        val row=textFile.map(row=>parse(row))
        //2ND

        val business_ids=row.map(x=>(x \ "business_id")).distinct().count()
//
        //3rd
        val years=args(3).trim()
        val year=row.filter(x=>(x \ "date").values.toString.contains(years))
//
//    //4th

        val user_ids=row.map(x=>((x \ "user_id").values.toString,1))
        val reduce_users=user_ids.reduceByKey((x,y)=>x+y)
        val sorts=reduce_users.sortBy(x=>(-x._2,x._1))

        //5th
        val excluded=List("(","[",",",".","!","?",":",";","]",")","")

        val stopWords=sc.textFile(args(2).trim())
        val all_words=stopWords.map(x=>x.trim().toLowerCase()).collect().toList
        val c=all_words:::excluded
        val text_rdd=row.map(x=>((x \ "text").values.toString)).flatMap(x=>x.toLowerCase().split(" "))


        val filter1=text_rdd.map(removePunc).filter(x=>x!=null&&x!=" "&&!c.contains(x.toLowerCase()))

        val stop_map=filter1.map(x=>(x.toLowerCase(),1))
        val stop_reduce=stop_map.reduceByKey((x,y)=>x+y)
        val stop_sort=stop_reduce.sortBy(x=>(-x._2,x._1)).map(x=>x._1).take(args(5).trim().toInt)

        val file=new File(args(1).trim())
        val bw=new BufferedWriter(new FileWriter(file))
        val map=Map("A"->row.count(),"B"->year.count(),"C"->business_ids,
        "D"->sorts.map(x=>List(JString(x._1),x._2)).take(args(4).trim().toInt),
        "E"->stop_sort.toList)

        bw.write(Json(DefaultFormats).write(map).replace(":",": ").replace(",",", "))
        bw.close()
        sc.stop()


        }
        def removePunc(key:String):String={
        val excluded="([,.!?:;])"
        var values=key
        for(a<-key){
        if(excluded.contains(a)){
        values=values.replace(a.toString,"")
        }
        }
        values.toLowerCase()
        }

        }
