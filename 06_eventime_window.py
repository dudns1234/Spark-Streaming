# Event time : 클라이언트에서 이벤트가 발생한 시간
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
        
    # read_trafiic_from_socket().writeStream.format("console").outputMode("append").start().awaitTermination()

    def aggregate_traffic_counts_by_slibing_window():
        traffic_df = read_trafiic_from_socket()
        
        window_by_hours = ( # 윈도우를 쓰는 이유가 시간대별로 구분해서 집계하기 위함
            traffic_df.groupby( # 그래서 윈도우함수는 groupby를 많이 씀.
                    F.window(
                        F.col("time"), # t시간에 대한 컬럼을 지정
                        windowDuration = "2 hours", # 윈도우 크기
                        slideDuration = "1 hour"
                    ).alias("time")
                ) 
                .agg(
                    F.sum("count").alias("count_by_time")
                )
        )
        
        window_by_hours.writeStream.format("console").outputMode("complete").start().awaitTermination()
    # aggregate_traffic_counts_by_slibing_window()
    
    def aggregate_traffic_counts_by_slibing_window_with_starttime_endtime():
        traffic_df = read_trafiic_from_socket()
        
        window_by_hours = ( # 윈도우를 쓰는 이유가 시간대별로 구분해서 집계하기 위함
            traffic_df.groupby( # 그래서 윈도우함수는 groupby를 많이 씀.
                    F.window(
                        F.col("time"), # t시간에 대한 컬럼을 지정
                        windowDuration = "2 hours", # 윈도우 크기
                        slideDuration = "1 hour"
                    ).alias("time")
                ) 
                .agg(
                    F.sum("count").alias("count_by_time")
                )
                .select(
                    F.col("time").getField("start").alias("start"),
                    F.col("time").getField("end").alias("end"),
                    F.col("count_by_time")
                )
                .orderBy(F.col("start"))
        )
        
        window_by_hours.writeStream.format("console").outputMode("complete").start().awaitTermination()
    # aggregate_traffic_counts_by_slibing_window_with_starttime_endtime()
    
    def traffic_per_hour_tumbling_window():
        traffic_df = read_trafiic_from_socket() # 소켓에서 스트리밍해야해서 불러와야함.
        
        window_by_hours = ( # 윈도우를 쓰는 이유가 시간대별로 구분해서 집계하기 위함
            traffic_df.groupby( # 그래서 윈도우함수는 groupby를 많이 씀.
                    F.window(
                        F.col("time"), # 시간에 대한 컬럼을 지정
                        "1 hour",
                    ).alias("time")
                ) 
                .agg(
                    F.sum("count").alias("count_by_time")
                )
                .select(
                    F.col("time").getField("start").alias("start"),
                    F.col("time").getField("end").alias("end"),
                    F.col("count_by_time")
                )
                .orderBy(F.col("start"))
        )
        
        window_by_hours.writeStream.format("console").outputMode("complete").start().awaitTermination()
    # traffic_per_hour_tumbling_window()
    
    def traffic_domain_per_hour():
        
        # 소켓 말고 이번엔 파일에서 스트리밍해볼것 
        def read_traffics_from_file():
            return(
                spark.readStream.schema(domain_traffic_schema)
                .json("/home/ubuntu/working/spark-examples/spark-streaming/data/traffics")
            )
            
        traffics_df = read_traffics_from_file() # 파일에서 스트리밍해야해서 불러와야함.
        
        df = (
            traffics_df.groupby(
                F.col("domain"), # 텀블링 윈도우 할것임.
                F.window(
                    F.col("time"),
                    "1 hour"
                ).alias("hour"))
            .agg(F.sum("count").alias("total_count"))
            .select(
                F.col("hour").getField("start").alias("start"),
                F.col("hour").getField("end").alias("end"),
                F.col("domain"),
                F.col("total_count")
            )
            .orderBy(F.col("hour"), F.col("total_count").desc())
        )
        
        df.writeStream.format("console").outputMode("complete").start().awaitTermination()

    traffic_domain_per_hour()