from pyspark.sql import SparkSession
import pyspark.sql.functions as F

if __name__ == "__main__":
    spark = (
        SparkSession.builder.master("local[2]")
            .appName("Streaming DataFrame Join")
            .getOrCreate()
    )
    
    # 1. 기본 배치 JOIN
    authors = spark.read.option("inferSchema", True).json(
        "/home/ubuntu/working/spark-examples/spark-streaming/data/authors.json"
    )
    
    books = spark.read.option("inferSchema", True).json(
        "/home/ubuntu/working/spark-examples/spark-streaming/data/books.json"
    )
    
    # 기준을 authors, join 수행할 데이터는 books
    authors_books_df = authors.join(books, authors['book_id'] == books['id'], 'inner')
    # authors_books_df.show()
    
    # 2. 1개의 데이터 프레임은 스트리밍 되고, 다른 한개의 데이터 프레임 배치
    def stream_join_with_static():
        
        streamed_books = (
            spark.readStream.format("socket")
                .option("host","localhost")
                .option("port",12345)
                .load()
                .select(
                    F.from_json(F.col("value"), books.schema).alias("book")
                )
                .selectExpr(
                    "book.id as id",
                    "book.name as name",
                    "book.year as year"
                )
        )
        # inner join 수행 시에는 반드시 왼쪽에 위치한 데이터 프레임이 "배치 데이터 프레임"이어야한다.
        # authors_books_df = authors.join(
        #     streamed_books, authors['book_id'] == streamed_books['id'], 'inner'
        # )
        
        # left join 수행 시에는 왼쪽의 데이터 프레임이 Streaming 데이터 프레임이 될 수도 있다.
        books_authors_df = streamed_books.join(
            authors, authors["book_id"] == streamed_books["id"], "left"
        )
        
        books_authors_df.writeStream.format("console").outputMode("append").start().awaitTermination()
        # authors_books_df.writeStream.format("console").outputMode("append").start().awaitTermination()
        #streamed_books.writeStream.format("console").outputMode("append").start().awaitTermination()        
    
    #stream_join_with_static()
    
    # 3. 양 쪽 모두 스트리밍 되는 경우
    def stream_join_with_stream():
        
        streamed_books = (
            spark.readStream.format("socket")
                .option("host","localhost")
                .option("port",9999)
                .load()
                .select(
                    F.from_json(F.col("value"), books.schema).alias("book")
                )
                .selectExpr(
                    "book.id as id",
                    "book.name as name",
                    "book.year as year"
                )
        )
        
        streamed_authors = (
            spark.readStream.format("socket")
                .option("host", "localhost")
                .option("port", 9998)
                .load()
                .select(
                    F.from_json(F.col("value"), authors.schema).alias("author")
                )
                .selectExpr(
                    "author.id as id", "author.name as name", "author.book_id as book_id"
                )
        )
        
        authors_books_df = streamed_authors.join(
            streamed_books, streamed_books["id"] == streamed_authors["book_id"], "inner"
        )
        
        authors_books_df.writeStream.format("console").outputMode("append").start().awaitTermination()
    
    stream_join_with_stream()    