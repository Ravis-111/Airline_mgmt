from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.window import Window

if __name__ == '__main__':
    spark = SparkSession.builder.appName("Airline management project").master("local[*]").getOrCreate()


    ###############    Read Files     ####################

    #    Airline data    #

    airline_schema = StructType([StructField("Airline_id", IntegerType()),
                                StructField("Name", StringType()),
                                StructField("Alias", StringType()),
                                StructField("IATA", StringType()),
                                StructField("ICAO", StringType()),
                                StructField("Callsign", StringType()),
                                StructField("Country", StringType()),
                                StructField("Active", StringType())
                                 ])

    airline_df = spark.read.csv(path=r"H:\---=BRAINWORKS DCE----\-----PYSPARK-------\--------------AIRLINE DATA----------\airline.csv",
                                schema=airline_schema)
    # airline_df.show()
    # airline_df.printSchema()

    #       Airport file     #

    airport_schema = aiport_schema = StructType([StructField("Airport_id", IntegerType()),
                                StructField("Name_Airport", StringType()),
                                StructField("City", StringType()),
                                StructField("Country", StringType()),
                                StructField("IATA_code", IntegerType()),
                                StructField("ICAO_Code", IntegerType()),
                                StructField("Latitude", DecimalType()),
                                StructField("Longitude", DecimalType()),
                                StructField("Altitude", IntegerType()),
                                StructField("Timezon", DecimalType()),
                                StructField("DST", StringType()),
                                StructField("Tz", StringType()),
                                StructField("Type_of_AP", StringType()),
                                StructField("Source", StringType())
                                ])

    airport_df = spark.read.csv(path=r"H:\---=BRAINWORKS DCE----\-----PYSPARK-------\--------------AIRLINE DATA----------\airport.csv",
                                schema=airport_schema)

    # airport_df.show()
    # airport_df.printSchema()

    #       Route file      #

    route_df = spark.read.parquet(r"H:\---=BRAINWORKS DCE----\-----PYSPARK-------\--------------AIRLINE DATA----------\routes.snappy.parquet")

    # route_df.show()

    #       Plane file        #

    plane_schema = StructType([
        StructField("Name", StringType()),
        StructField("IATA_code", StringType()),
        StructField("ICAO_Code", StringType()),
        ])

    plane_df = spark.read.csv(r"H:\---=BRAINWORKS DCE----\-----PYSPARK-------\--------------AIRLINE DATA----------\plane.csv",schema=plane_schema,header=True)

    # plane_df.show()
    # plane_df.printSchema()


    ####  ---QUESTIONS---  ###

    # 1. In any of your input file if you are getting
    # \N or null values in your column and that
    # column is of string type then put default
    # value as "(unknown)" and if column is of type
    # integer then put -1 .

    airline_ans1 = airline_df.na.fill("(Unknown)").na.replace(("\\N"),"(Unknown)")
    # airline_ans1.show()

    airport_ans1 = airport_df.na.fill(value=-1)
    # airport_ans1.show()

    route_ans1 = route_df.withColumn("codeshare",when(route_df.codeshare.isNull(),"(Unknown)")\
                                     .otherwise("codeshare")).na.replace("\\N","(Unknown)")
    # route_ans1.show()

    plane_ans1 = plane_df.na.replace("\\N", "(Unknown)")
    # plane_ans1.show()

    # 2. find the country name which is having both airlines and airport .

    # airline_ans1.join(airport_ans1,on="Country",how="inner").select(airline_ans1.Country.alias("country_name"))\
    # .distinct().orderBy("country_name").show()

    #      sql method   #

    airline_ans1.createOrReplaceTempView("airline")
    airport_ans1.createOrReplaceTempView("airport")
    route_ans1.createOrReplaceTempView("route")


    # spark.sql("select distinct (Country) from airport,airline where airline.Country=airport.Country order by Country").show()


    #Que. 3a.get the airlines details like name, id,.
    # which is has taken takeoff more than 3 times from same airport .

    # airline_ans1.join(route_ans1, on ="Airline_id", how ="inner") .groupBy("Airline_id", "Name", "src_airport")\
    # .agg(count("src_airport").alias("takeoff")).filter(col("takeoff")>3).orderBy(col("takeoff")).show()

    #       sql method     #

    # spark.sql("select ai.Airline_id,ai.Name,src_airport,count(*) takeoff from airline ai inner join route rt on ai.Airline_id=rt.Airline_id"
    #           " group by ai.Airline_id,ai.Name,src_airport having count(*) > 3 order by takeoff").show()


    #Que. 3b.get airport details which has minimum number of takeoffs and landing.

    # takeoff = airport_ans1.join(route_ans1,airport_ans1.Airport_id==route_ans1.src_airport_id,"inner")\
    #     .groupBy("Airport_id","Name_Airport","src_airport").count()

    # minimum = takeoff.agg(min("count")).take(1)[0][0]

    # takeoff.filter(col("count")==minimum).show()



    # landing = airport_ans1.join(route_ans1,airport_ans1.Airport_id==route_ans1.dest_airport_id,"inner")\
    #     .groupBy("Airport_id","Name_Airport","dest_airport").count()
    #
    # minimum_ld = landing.agg(min("count")).take(1)[0][0]
    # landing.filter(col("count")==minimum_ld).show()

    # airport_ans1.join(route_ans1, airport_ans1.Airport_id==route_ans1.src_airport_id,"inner")\
    #     .groupBy("Airport_id","Name_Airport","src_airport","dest_airport")\
    #     .agg(count("src_airport").alias("takeoff")),count("dest_airport").alias("landing")\
    #     .orderBy(col("takeoff").asc(),col("landing").asc()).limit(1).show()

    #   sql method  #

    # spark.sql("select Airport_id,Name_Airport,src_airport,dest_airport,count(src_airport) takeoff,count(dest_airport) landing "
    #           " from airport inner join route on airport.Airport_id=route.src_airport_id"
    #           " group by Airport_id,Name_Airport,src_airport,dest_airport"
    #           " order by takeoff asc,landing asc").show()

    # Que.4. get airport details which is having maximum number of takeoff and landing.

    # takeoff = airport_ans1.join(route_ans1, airport_ans1.Airport_id==route_ans1.src_airport_id,"inner")\
    #                 . groupBy("Airport_id","Name_Airport","src_airport").count()

    # maximum_tk =takeoff.agg(max("count")).take(1)[0][0]
    # takeoff.filter(col("count")==maximum_tk).show()

    # landing = airport_ans1.join(route_ans1,airport_ans1.Airport_id==route_ans1.src_airport_id,"inner")\
    #         . groupBy("Airport_id", "Name_Airport", "dest_airport").count()

    # maximum_ld = landing.agg(max("count")).take(1)[0][0]
    # landing.select("Airport_id", "Name_Airport", "dest_airport","count").filter(col("count")==maximum_ld).show()

    # airport_ans1.join(route_ans1,airport_ans1.Airport_id==route_ans1.src_airport_id,"inner")\
    #         . groupBy("Airport_id", "Name_Airport", "src_airport", "dest_airport")\
    #         . agg(count("src_airport").alias("takeoff"), count("dest_airport").alias("landing"))\
    #         . orderBy(col("takeoff").desc(), col("landing").desc()).limit(1).show()

    #       sql method      #
    # spark.sql(" select Airport_id,Name_Airport,src_airport,dest_airport,count(src_airport) takeoff,count(dest_airport) landing"\
    #           " from airport  inner join route  on airport.Airport_id=route.src_airport_id"
    #           " group by Airport_id,Name_Airport,src_airport,dest_airport"
    #           " order by takeoff desc,landing desc").show()



    # Que.6. Get the airline details, which is having direct flights. details like airline id,
    # name, source airport name, and destination airport name.

    # airline_ans1.join(route_ans1 ,on ="Airline_id", how="inner")\
    # . select("Airline_id","Name","src_airport","dest_airport","stops").filter(col("stops")==0).show()

    #       sql method      #

    # spark.sql("select ai.Airline_id,Name,src_airport,dest_airport,stops " +
              # "from airline ai inner join route rt on ai.airline_id=rt.airline_id where stops=0").show()





















