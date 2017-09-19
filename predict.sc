val model = MatrixFactorizationModel.load(sc, "dbfs:/millionsong/model8000")

val userTracksPredictionDecart = sqlContext.read.parquet("dbfs:/millionsong/userTracksModelReady.parquet")
// val userTracksPredictionDecart = sqlContext.read.parquet("dbfs:/millionsong/userTracksPrediction.parquet")

// display(userTracksPredictionDecart.sort($"user".desc).take(500))

// Evaluate the model on rating data
// val userTracksPredictionRdd = userTracksPredictionDecart.rdd.map(row =>(row.getInt(3), row.getInt(2)))
val userTracksPredictionRdd = userTracksPredictionDecart.rdd.map(row => (row.getInt(3), row.getInt(4)))
val predictions =
  model.predict(userTracksPredictionRdd).map { case Rating(userId, trackId, rating) =>
    (userId, trackId, rating)
  }
dbutils.fs.rm("dbfs:/millionsong/predictions", true)
predictions.saveAsTextFile("dbfs:/millionsong/predictions")
predictions.sortBy(x => -x._3).take(1000).foreach(println)
println("predictions count = " + predictions.count())

//==============================================================
Result:

(263,424,2.3719417051556158)
(263,425,2.3719417051556158)
(263,426,2.3719417051556158)
(263,419,2.3719417051556158)
(263,427,2.3719417051556158)
(263,428,2.3719417051556158)
(263,420,2.3719417051556158)
(263,421,2.3719417051556158)
(263,422,2.3719417051556158)
(263,423,2.3719417051556158)
(115,184,2.364270225273529)
(115,185,2.364270225273529)
(115,186,2.364270225273529)
(115,187,2.364270225273529)
(115,188,2.364270225273529)
(115,189,2.364270225273529)
(115,181,2.364270225273529)
(115,190,2.364270225273529)
(115,182,2.364270225273529)
(115,183,2.364270225273529)
(359,592,2.2201296923096328)
...
predictions count = 691456
model: org.apache.spark.mllib.recommendation.MatrixFactorizationModel = org.apache.spark.mllib.recommendation.MatrixFactorizationModel@644e4067
userTracksPredictionDecart: org.apache.spark.sql.DataFrame = [user: string, track: string ... 5 more fields]
userTracksPredictionRdd: org.apache.spark.rdd.RDD[(Int, Int)] = MapPartitionsRDD[485] at map at command-4173173537186302:10
predictions: org.apache.spark.rdd.RDD[(Int, Int, Double)] = MapPartitionsRDD[494] at map at command-4173173537186302:12
