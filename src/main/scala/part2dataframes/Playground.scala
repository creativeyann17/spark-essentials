package part2dataframes

import org.apache.spark.sql.functions.{col, column, expr}
import org.apache.spark.sql.{Row, SaveMode, SparkSession}
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}

object Playground extends App{

  // creation du SparkSession, obligatoire partout partout
  val spark = SparkSession.builder
    .config("spark.sql.legacy.timeParserPolicy","LEGACY")
    .appName("MY-SPARK-SESSION")
    .config("spark.master","local") //local[*] spark://10.5.0.2:7077
    .getOrCreate()

  // reading a DF
  val firstDF = spark.read
    .format("json")
    .option("inferSchema","true") // le schema va etre generé tout seul a partir des data, mieux eviter et faire manuellement
    .load("src/main/resources/data/cars.json")

  // display the DF in console
  firstDF.show

  // display the schema in console
  firstDF.printSchema()

  // first 10 rows of the DF
  firstDF.take(10).foreach(println) // on remarque que chaque Row est un Array (nice)

  // spark types
  val longType = LongType // comme Scala mais avec suffix *Type pour tout ceux qui existent

  // schema is a StructType
  val customSchema = StructType(
    Array(
      StructField("Name", StringType, nullable = true)
    )
  )

  // recupération du schema généré par inferSchema
  val carsDFSchema = firstDF.schema
  println(carsDFSchema)

  // read DF with my schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    .load("src/main/resources/data/cars.json")

  // create rows by hand
  val myRow = Row("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")

  // create a DF from implicit tuples, mais ca foncitonne aussi avec Row
  val cars= Seq(
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"), //Row(..)
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA"),
    ("chevrolet chevelle malibu",18.0,8L,307.0,130L,3504L,12.0,"1970-01-01","USA")
  )

  val manualCarsDF = spark.createDataFrame(cars) // schema auto-inferred car c'est typé vu qu'on a ecrit en scala

  // note: DFs have schemas, rows do not
  // create DFs with implicits
  import spark.implicits._
  // contient un implicit qui transforme Seq en DF
  val manualCarsDFWithImplicits = cars.toDF("Name", "MPG", "Cylenders", "Displacements", "HP", "Weight", "Acceleration", "Year of origin", "Country")

  manualCarsDF.printSchema()
  manualCarsDFWithImplicits.printSchema()

  // -------------------------------------------------------------------------------
  // 1. create a manual DF describing smartphones
  // 2. read another file from data folder (movies.json)
  //    - print schema + count nb of rows

  val smartphonesDF = spark.createDataFrame(Seq(
    ("","",""),
    ("","","")
  ))

  val moviesDF = spark.read.format("json").option("inferSchema",true).load("src/main/resources/data/movies.json")
  moviesDF.printSchema()
  println(moviesDF.count())

  // -------------------------------------------------------------------------------

  /*
  Reading a DF
  - format
  - schema (optional) or inferSchema = true
  - zero or more options
   */
  val carsDF = spark.read
    .format("json")
    .schema(playground.Playground.carsSchema)
    .options(Map(
      ("mode","failFast"),  // dropMalformed = ignore les row pas belles, permissive (default)
      ("path","src/main/resources/data/cars.json")// can be a S3 file
    ))
    .load()

  println("-------------------------------------------------------------------------")
  carsDF.show() // trigger Spark to load the DF because all the stuff is lazy

  // writing DFs
  /*
  - format
  - save mode / override / ignore / error if exist
  - path
  - options
   */
  carsDF.write
    .format("json")
    .mode(SaveMode.Overwrite)
    .save("src/main/resources/data/cars_dupe.json")

  val carsSchemaWithDate = StructType(Array(
    StructField("Name", StringType),
    StructField("Miles_per_Gallon", DoubleType),
    StructField("Cylinders", LongType),
    StructField("Displacement", DoubleType),
    StructField("Horsepower", LongType),
    StructField("Weight_in_lbs", LongType),
    StructField("Acceleration", DoubleType),
    StructField("Year", DateType),
    StructField("Origin", StringType)
  ))

  // JSON flags
  spark.read
    .schema(carsSchemaWithDate)
    .option("dateFormat","YYYY-MM-dd") // if Spark failed parsing => null
    .option("allowSingleQuotes",true)
    .option("compression","uncompressed") // bzip2, gzip, lz4, snappy, deflate, default
    .json("src/main/resources/data/cars.json")

  // CSV flags
  val stocksSchema = StructType(Array(
    StructField("symbol", StringType),
    StructField("date", DateType),
    StructField("price", DoubleType)
  ))

  val stocksDF = spark.read
    .schema(stocksSchema)
    .option("dateFormat", "MMM dd YYYY")
    .option("header", true)
    .option("sep", ",")
    .option("nullValue", "")
    .csv("src/main/resources/data/stocks.csv")

  stocksDF.show()

  // Parquet
  carsDF.write.mode(SaveMode.Overwrite).parquet("src/main/resources/data/cars.parquet")

  // Test files
  spark.read.text("src/main/resources/data/sample_text.txt").show() // ligne / ligne cool

  // reading from remote DB (lancer prosgres)
  val employeesDF = spark.read
    .format("jdbc")
    .options(Map(
      ("driver" , "org.postgresql.Driver"),
      ("url" , "jdbc:postgresql://localhost:5432/rtjvm"),
      ("user" , "docker"),
      ("password" , "docker"),
      ("dbtable","public.employees")
    )).load()

  employeesDF.show()
  employeesDF.write
    .format("jdbc")
    .mode(SaveMode.Overwrite)
    .options(Map(
      ("driver" , "org.postgresql.Driver"),
      ("url" , "jdbc:postgresql://localhost:5432/rtjvm"),
      ("user" , "docker"),
      ("password" , "docker"),
      ("dbtable","public.employees_saved")
    )).save()

  println("-------------------------------------------------------------------------")

  // Columns
  val firstCol = carsDF.col("Name")

  // Selecting (projection)
  val carNamesDF = carsDF.select(firstCol)

  carNamesDF.show()

  // various
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    'Year, // ' auto convert to column
    $"Horsepower", // interpolated string
    expr("Origin") // expression (regex j'imagine)
  )

  carsDF.select("Name", "Year") // AHHHHH nice

  // EXPRESSIONS
  val simplestExpression = carsDF.col("Weight_in_lbs")
  val weightInKgExpression = carsDF.col("Weight_in_lbs") / 2.2
  val carsWithWeightsDF = carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    weightInKgExpression,
    expr("Weight_in_lbs / 2.2").as("Weight_in_kg2")
  )
  carsWithWeightsDF.show()
  // selectExpr
  var cardsWithSelectExpr = carsDF.selectExpr(
    "Name","Weight_in_lbs","Weight_in_lbs / 2.2"
  )
  cardsWithSelectExpr.show()

  // DF processing
  carsDF.withColumn("Weight_in_kg3", col("Weight_in_lbs") / 2.2).show()

  val carsDFWithColumnRenamedDF = carsDF.withColumnRenamed("Weight_in_lbs","Weight in pounds")
  carsDFWithColumnRenamedDF.show()

  // attention au char speciaux
  carsDFWithColumnRenamedDF.selectExpr("`Weight in pounds`").show()

  // remove
  carsDF.drop("Cylinders", "Displacement").show()

  carsDF.filter(col("Origin") =!= ("USA")).show() // =!= ===
  carsDF.filter(col("Origin").notEqual("USA")).show() // where()
  carsDF.filter("Origin != 'USA'").show()
  carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") >150).show()
  carsDF.filter( col("Origin") === "USA"  and col("Horsepower") >150).show()
  carsDF.filter("Origin == 'USA' and Horsepower > 150").show()

  // add rows
  val moreCarsDF = spark.read.schema(playground.Playground.carsSchema).json("src/main/resources/data/cars.json")
  carsDF.union(moreCarsDF).show()

  // distinct
  carsDF.select("Origin").distinct().show()

  // aggregations and grouping
  // counting
  import org.apache.spark.sql.functions.{count, col, countDistinct, approx_count_distinct, min, sum, avg, mean, stddev}
  moviesDF.select(count(col("Major_Genre"))).show() // all values not null
  moviesDF.selectExpr("count(Major_Genre)").show()
  moviesDF.select("*").show() // includes null
  moviesDF.select(countDistinct(col("Major_Genre"))).show()
  moviesDF.select(approx_count_distinct(col("Major_Genre"))).show() // okey ... strange
  moviesDF.select(min("IMDB_Rating")).show()
  moviesDF.selectExpr("min(IMDB_Rating)").show()
  moviesDF.select(sum("US_Gross")).show()
  moviesDF.select(avg("Rotten_Tomatoes_Rating")).show()

  moviesDF.select(
    mean("Rotten_Tomatoes_Rating"),
    stddev("Rotten_Tomatoes_Rating")
  ).show()

  // grouping
  moviesDF.groupBy("Major_Genre").count().show() // select count(*) from moviesDF group by Major_Genre
  moviesDF.groupBy("Major_Genre").avg("IMDB_Rating").show()
  moviesDF.groupBy("Major_Genre").agg(
    count("*").as("N_Movies"),
    avg("IMDB_Rating").as("Avg_Rating")
  ).orderBy("Avg_Rating").show()

  // joins
  val guitarsDF = spark.read.option("inferSchema",true).json("src/main/resources/data/guitars.json")
  val guitaristsDF = spark.read.option("inferSchema",true).json("src/main/resources/data/guitarPlayers.json")
  val bandsDF = spark.read.option("inferSchema",true).json("src/main/resources/data/bands.json")

  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner") // ignore null
  guitaristBandsDF.show
  guitaristsDF.join(bandsDF, joinCondition, "left_outer").show // all left even if right is null
  guitaristsDF.join(bandsDF, joinCondition, "right_outer").show // all right even if left null
  guitaristsDF.join(bandsDF, joinCondition, "outer").show // all even if null
  guitaristsDF.join(bandsDF, joinCondition, "left_semi").show // only left non-null part remove the right
  guitaristsDF.join(bandsDF, joinCondition, "left_anti").show // only missing join with right
  // if we have 2 columns with same name after the join
  // rename column
  guitaristsDF.join(bandsDF.withColumnRenamed("id","band"), "band").show
  // drop duplicated column
  guitaristBandsDF.drop(bandsDF.col("id"))

}
