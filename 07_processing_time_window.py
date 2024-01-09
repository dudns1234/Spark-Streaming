# processing time : 실제 스파크 어플리케이션에 이벤트가 들어온 시간
#  데이터에 기록된 시간정보를 활용하는 것이 아닌, current_timestamp 같은 현 시간 함수를 활용해서 집계

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = SparkSession(
        SparkSession.builder.master("local[2]")
            .appName("Processing time windows")
            .getOrCreate()
    )
    
    def aggregate_by_processing_time():
        line_char_count_by_window_df = (  # 텍스트 길이 제기??
            spark.readStream.format("socket")
                .option("host","localhost")
                .option("port",12345)
                .load()
                .select(
                    F.col("value"),
                    F.current_timestamp().alias("processingTime")
                )
                .groupby(
                    F.window(
                        F.col("processingTime"),
                        "5 seconds"
                    ).alias("window")
                )
                .agg(
                    F.sum(F.length(F.col("value"))).alias("charCount")
                )
                .select(
                    F.col("window").getField("start").alias("start"),
                    F.col("window").getField("end").alias("end"),
                    F.col("charCount")
                )
                .orderBy(F.col("start").desc())
        )
        
        line_char_count_by_window_df.writeStream.format("console").outputMode("complete").start().awaitTermination()
        
    aggregate_by_processing_time()    