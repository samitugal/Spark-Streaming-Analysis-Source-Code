import spark.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions
import java.sql.Timestamp
import org.apache.spark.sql.streaming.Trigger.ProcessingTime

val bucket = "sparkidsa"
spark.conf.set("temporaryGcsBucket", bucket)
spark.conf.set("parentProject", "apt-federation-350820")

val kafkaDF = spark.readStream.format("kafka").option("kafka.bootstrap.servers","localhost:9092").option("subscribe","project").load
val schema = StructType(List(StructField("BranchID",IntegerType),StructField("ProductKey",IntegerType),StructField("TimeStamp",LongType),StructField("CustomerID",IntegerType)))
val activationDF = kafkaDF.select(from_json($"value".cast("string"),schema).alias("activation"))
val modelCountDF = activationDF.groupBy($"activation"("BranchID"),$"activation"("ProductKey"),$"activation"("CustomerID"),$"activation"("TimeStamp")).count.sort($"count".desc)
val modelCountQuery = modelCountDF.writeStream.outputMode("complete").format("bigquery").option("table","idsaproject.Fact").option("checkpointLocation", "/path/to/checkpoint/dir/in/hdfs/new342345331213").option("failOnDataLoss",false).option("truncate",false).start().awaitTermination()