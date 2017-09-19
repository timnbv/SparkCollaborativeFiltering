import org.apache.spark.mllib.recommendation.ALS
import org.apache.spark.mllib.recommendation.MatrixFactorizationModel
import org.apache.spark.mllib.recommendation.Rating

val userTracksModelReady = sqlContext.read.parquet("dbfs:/millionsong/userTracksModelReady.parquet")
// display(userTracksModelReady.sort($"rating".desc).limit(500))
// display(userTracksModelReady.where($"userId"===263).take(500))
val rank = 10
val ratingsRdd = userTracksModelReady.rdd.map(row => Rating(row.getInt(3), row.getInt(4), row.getDouble(6)))
val numIterations = 10
val model = ALS.train(ratingsRdd, rank, numIterations, 0.01)
dbutils.fs.rm("dbfs:/millionsong/model8000", true)
model.save(sc, "dbfs:/millionsong/model8000")
display(dbutils.fs.ls("dbfs:/millionsong/model8000/data/product"))