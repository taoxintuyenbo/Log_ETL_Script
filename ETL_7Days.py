import findspark
findspark.init()
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession
from pyspark.sql.functions import *
import pandas as pd
from pyspark.sql.window import Window
import pyspark.sql.functions as sf
from pyspark.sql.functions import concat_ws
from datetime import datetime, timedelta
import os

spark = SparkSession.builder.config("spark.driver.memory", "6g").config("spark.executor.cores",6).getOrCreate()

def category_AppName(df):
    df=df.withColumn("Type",when(col("AppName")=="CHANNEL","Truyen Hinh")
        .when(col("AppName")=="RELAX","Giai Tri")
        .when(col("AppName")=="CHILD","Thieu Nhi")
        .when((col("AppName")=="FIMS")|(col("AppName")=="VOD"),"Phim Truyen")
        .when((col("AppName")=="KPLUS")|(col("AppName")=="SPORT"),"The Thao")
    )
    df = df.select('Contract','Type','TotalDuration')
    df = df.filter(df.Contract != '0' )
    df = df.filter(df.Type != 'Error')
    return df

def most_watch(df):
    df=df.withColumn("MostWacth",greatest(col("Giai Tri"),col("Phim Truyen"),col("The Thao"),col("Thieu Nhi"),col("Truyen Hinh"),col("Giai Tri")))
    df=df.withColumn("MostWacth",
                    when(col("MostWacth")==col("Truyen Hinh"),"Truyen Hinh")
                    .when(col("MostWacth")==col("Phim Truyen"),"Phim Truyen")
                    .when(col("MostWacth")==col("The Thao"),"The Thao")
                    .when(col("MostWacth")==col("Thieu Nhi"),"Thieu Nhi")
                    .when(col("MostWacth")==col("Giai Tri"),"Giai Tri"))
    return df

def customer_taste(df):
    df=df.withColumn("Taste",concat_ws("-",
                                    when(col("Giai Tri").isNotNull(),lit("Giai Tri"))
                                    ,when(col("Phim Truyen").isNotNull(),lit("Phim Truyen"))
                                    ,when(col("The Thao").isNotNull(),lit("The Thao"))
                                    ,when(col("Thieu Nhi").isNotNull(),lit("Thieu Nhi"))
                                    ,when(col("Truyen Hinh").isNotNull(),lit("Truyen Hinh"))))
    return df

def convert_to_datevalue(string):
    date_value=datetime.strptime(string,"%Y%m%d").date()
    return date_value
    
def convert_to_stringvalue(date):
    string_value = date.strftime("%Y%m%d")    
    return string_value

def date_range(start_date,end_date):
    date_list=[]
    current_date=start_date
    while(current_date<=end_date):
        date_list.append(convert_to_stringvalue(current_date))
        current_date+=timedelta(days=1)
    return date_list

def generate_range_date(start_date,end_date):
    start_date= convert_to_datevalue(start_date)
    end_date= convert_to_datevalue(end_date)
    date_list=date_range(start_date,end_date)
    return date_list

def find_active(df):
    windowspec = Window.partitionBy("Contract")
    df=df.withColumn("Active",sf.count("Date").over(windowspec))
    df=df.drop("Date")
    df=df.withColumn("Active",when(col("Active")>4,"High").otherwise("Low"))
    df=df.groupBy("Contract").agg(
    sf.sum("Giai Tri").alias("Total_Giai_Tri"),
    sf.sum("Phim Truyen").alias("Total_Phim_Truyen"),
    sf.sum("The Thao").alias("Total_The_Thao"),
    sf.sum("Thieu Nhi").alias("Total_Thieu_Nhi"),
    sf.sum("Truyen Hinh").alias("Total_Truyen_Hinh"),
    sf.first("MostWacth").alias("MostWacth"),
    sf.first("Taste").alias("Taste"),
    sf.first("Active").alias("Active")
)
    return df

def ETL_1_DAY(path,path_day):
    print('------------------------')
    print('Read data from Json file')
    print('------------------------')
    df= spark.read.json(path+path_day+".json")
    print('------------------------')
    print('Category AppName')
    print('------------------------') 
    df=df.select("_source.*")
    df=category_AppName(df)
    print('-----------------------------')
    print('Pivoting data')
    print('-----------------------------')
    df=df.groupBy("Contract").pivot("Type").sum("TotalDuration")
    print('-----------------------------')
    print('Find most watch')
    print('-----------------------------') 
    df=most_watch(df)
    print('-----------------------------')
    print('Find customer taste')
    print('-----------------------------') 
    df=customer_taste(df)
    df=df.withColumn("Date", to_date(lit(path_day), "yyyyMMdd"))
    print('-----------------------------')
    print('Find avtive ')
    print('-----------------------------') 
    df=find_active(df)
    return df


def import_to_mysql(result):
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'study_data'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = ''
    result.write.format('jdbc').option('url',url).option('driver',driver).option('dbtable','summary_behavior_data').option('user',user).option('password',password).mode('overwrite').save()
    return print("Data Import Successfully")

def maintask(path,save_path):
    dir_list=os.listdir(path)
    # start_date=input("Nhap ngay bat dau: ")
    # end_date=input("Nhap ngay ket thuc: ")
    start_date="20220401"
    end_date="20220407"
    date_list=generate_range_date(start_date,end_date)
    print("ETL data file: "+date_list[0]+".json")
    result=ETL_1_DAY(path,date_list[0])
    for x in date_list[1:]:
        print("ETL data file: "+x+".json")
        result=result.union(ETL_1_DAY(path,x))
                
    print('-----------------------------')
    print('Showing data')
    print('-----------------------------') 
    result.show(10)
    print('-----------------------------')
    print('Saving csv output')
    print('-----------------------------')
    result.repartition(1).write.csv(save_path, mode='overwrite', header=True)
    print('-----------------------------')
    print('Import result to mysql')
    print('-----------------------------')
    import_to_mysql(result)
    print("Finished job")
    return result


path="/Users/nguyentadinhduy/Documents/SQL_THLONG/DE_Gen5_Bigdata/CLASS4/DataRaw/"
save_path="/Users/nguyentadinhduy/Documents/SQL_THLONG/DE_Gen6_Bigdata/Class3/DataClean"
df=maintask(path,save_path)
