from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField,  StringType, IntegerType, TimestampType # StructType : 전체 데이터프레임을 구성하기위함, StructField : 컬럼구조
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[2]")
            .appName("EventTime Window").getOrCreate()
    )
    
    domain_traffic_schema = StructType([
        StructField("id", StringType(), False),
        StructField("domain", StringType(), False),
        StructField("count", IntegerType(), False),
        StructField("time", TimestampType(), False),
    ])
    
    # 스트리밍 데이터프레임 얻기
    def read_trafiic_from_socket(): # 소켓으로 데이터 주고 받을 것임
        return (
            spark.readStream.format("socket")
                .option("host","localhost")
                .option("port", 12345)
                .load()
                .select(F.from_json(F.col("value"), domain_traffic_schema).alias("traffic")) # json 데이터 불러오기 위함
                .selectExpr("traffic.*")
        )
    traffics_df = read_trafiic_from_socket()
    
    agg_df = (
        # 워터마크는 그룹바이하기 전에 만들어줌
        traffics_df.withWatermark("time", "10 minutes") # 워터마크는 10분
            .groupby(F.window(F.col("time"),"5 minutes").alias("time")) # 텀블링 윈도우는 5분
            .agg(F.sum("count").alias("total_count"))
            .select(
                F.col("time").getField("start").alias("start"),
                F.col("time").getField("end").alias("end"),
                F.col("total_count")
            )   
    )
    
    agg_df.writeStream.format("console").outputMode("update").start().awaitTermination()
    
    # read_trafiic_from_socket()