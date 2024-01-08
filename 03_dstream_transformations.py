from pyspark.sql import SparkSession
from pyspark.streaming import StreamingContext

import os
# 각 필드에 컬럼명을 부여하기 위한 클래스 설정
from collections import namedtuple
columns = ["Ticker","Date","Open","High","Low","Close","AdjClose","Volume"]
Finance = namedtuple("Finance", columns)

if __name__ == "__main__": # main함수 만들어주는 습관!!!
    sc = (
        SparkSession.builder.master("local[2]")
            .appName("DStream Transformations")
            .getOrCreate().sparkContext
    )
    
    ssc = StreamingContext(sc, 5) # csv파일을 쉼표로 split한다. 한줄이 들어오면 쉽표로 나눠서 필드로 관리
    
    # 옛날에서 파일에 데이터를 불러와서 원하는걸 뽑아옴
    # 지금은 스트리밍 되고 있는 데이터
    
    # 소켓으로부터 데이터를 스트리밍 하되, 컬럼 mapping까지 수행
    def read_finance():
        
        # ","를 기준으로 쪼갠 한 줄의 각 필드마자 컬럼 이름을 부여
        def parse(line):
            arr = line.split(",")
            return Finance(*arr)

        return ssc.socketTextStream("localhost", 12345).map(parse) # 파일 통해서 보내는게 아니라 socket을 통해서 보낼 것. 그리고 map을 통해 파싱처리를 해줄거다(map은 action이 아니고 transformation이여서 처리는 안됨)

    finance_stream = read_finance()
    # Finance_stream.pprint()
    
    def filter_nvda():
        finance_stream.filter(lambda f : f.Ticker == "NVDA").pprint()
    
    # filter_nvda()

    # Volume이 500만 이상인 종목에 대해서만 필터링
    def filter_volume():
        finance_stream.filter(lambda f : int(f.Volume) >= 5000000).pprint()
        
    # filter_volume()
    
    # 종목별 집계, 전체로 집계가 아닌, 마이크로 배치별로만 집계됨
    def count_ticker():
        # 스트리밍 환경에서 집계를 하면 전체 데이터에 대한 집계가 아닌, 마이크로 배치 단위 별로 집계가 수행
        finance_stream.map(lambda f: (f.Ticker, 1)).reduceByKey(lambda x, y : x + y).pprint()    
        
    # count_ticker()
    
    # 실습. groupByKey를 이용해서 날짜(Date)별 볼륨(Volume) 합계 구하기. 합계는 파이썬 내장함수인 sum을 이용
    def group_by_date_volume():
        finance_stream.map(lambda f : (f.Date, int(f.Volume))).groupByKey().mapValues(sum).pprint()
        
    # group_by_date_volume()
    
    # json형태로??
    def save_to_json():
        # Woker에서만 작동하는 함수를 작성(SAVE)
        
        def foreach_func(rdd): # worker에서만 작동하는 함수 / 거의 rdd가 마이크로배치라고 생각하면됨.
            # rdd가 비어있으면 아무것도 하지 않기
            if rdd.isEmpty():
                print("RDD IS EMPTY")
                return
            
            # 데이터가 rdd에 존재하는 경우에는 DataFrame으로 바꿔서 json으로 저장
            df = rdd.toDF(columns)
            
            dir_path = "/home/ubuntu/working/spark-examples/spark-streaming/data/stocks/outputs"
            
            # 경로 내에 몇 개의 파일이 있는지 구하고, 인덱스 생성
            n_files = len(os.listdir(dir_path))
            save_file_path = f"{dir_path}/finance-{n_files}.json"
            
            # json 형식으로 저장
            df.write.json(save_file_path)
            print("Write Complete")
            
        # 특정 Task를 Worker에서 작동하게 하기 - foreache RDD
        finance_stream.foreachRDD(foreach_func)
        
    save_to_json()
#     dtream : rdd가 순서대로 쌓여있는 것
# rdd : worker들을 하나의 rdd로
    ssc.start()
    ssc.awaitTermination()
    
    