import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._
import org.apache.spark.sql._
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.io.Text
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat


val userTracksTmp = sqlContext.read.format("csv")
  .option("delimiter", "\t")
  .load("dbfs:/millionsong/jam_to_msd.txt")
val colNames = Seq("user", "track")
val userTracks = userTracksTmp.toDF(colNames:_*)
// user_tracks.printSchema

var userTracksModel = userTracks.limit(1000)
var userTracksPrediction = userTracks.limit(1200).except(userTracksModel).toDF()
println("model count = " + userTracksModel.count())
val userTracksAll = userTracksModel.union(userTracksPrediction).limit(1000000)
println("prediction count = " + userTracksPrediction.count())
userTracksModel = userTracksModel.withColumn("has_track", lit(1))
// display(userTracksModel.take(10))
// userTracksModel.printSchema

var userCount = userTracksModel.groupBy($"user")
  .agg(countDistinct($"track") as "user_count")
  .sort($"user_count".desc)
// // display(userCount.take(200))

val int_id = udf((long_id: Long) => {
  long_id.toInt
})

var users = userTracksAll.select($"user").distinct().toDF()
//users  = users.withColumn("userId", int_id(monotonically_increasing_id()))
// display(users.take(1000))
// println("users count = " + users.count())

 var tracks = userTracksAll.select($"track").distinct()//.withColumn("trackId", int_id(monotonically_increasing_id()))
println("tracks count = " + tracks.count())
// display(tracks.take(100))

var userTracksModelDecart = users.crossJoin(tracks).join(userCount, Seq("user"))
users  = users.withColumn("userId", int_id(monotonically_increasing_id()))
tracks = tracks.withColumn("trackId", int_id(monotonically_increasing_id()))
userTracksModelDecart = userTracksModelDecart.join(users, Seq("user")).join(tracks, Seq("track"))
println("userTracksModelDecart count = " + userTracksModelDecart.count())

userTracksModelDecart = userTracksModelDecart.join(userTracksModel, Seq("user", "track"), "left_outer")

val k = 0.05
val func_rating = udf((user_count: Integer, has_track: Integer) => {
  if (has_track == null) user_count * k 
  else  5 
})

val userTracksModelReady = userTracksModelDecart.withColumn("rating", func_rating(col("user_count").cast("int"), col("has_track").cast("int")))
dbutils.fs.rm("dbfs:/millionsong/userTracksModelReady.parquet", true)
userTracksModelReady.write.parquet("dbfs:/millionsong/userTracksModelReady.parquet")

// userTracksModelReady.printSchema
//  display(userTracksModelReady.sort($"user".desc).take(50))

//=================================================================================
userTracksPrediction = userTracksPrediction.join(tracks, Seq("track")).join(users, "user")
// println("userTracksPrediction count = " + userTracksPrediction.count())
display(userTracksPrediction.sort($"userId").limit(10000000))
dbutils.fs.rm("dbfs:/millionsong/userTracksPrediction.parquet", true)
userTracksPrediction.write.parquet("dbfs:/millionsong/userTracksPrediction.parquet")



