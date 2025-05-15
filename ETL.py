#Libs
import logging as log, os
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql.functions import col,when,lit,greatest,concat_ws
import target_connections as tar_conn
from pyspark.sql.window import Window
import pyspark.sql.functions as sf
# Logging configuration
log.basicConfig(
    filename="log.txt",
    level=log.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    encoding='utf-8'
)

spark = SparkSession.builder.getOrCreate()

DATA_PATH = "D:\\Personal_Doc\\study_DE\\BigData\\log_content\\"
OUTPUT_PATH = "D:\\Personal_Doc\\Class5-OLAPOutput\\"
def getFilesToProcess(start_date='20220401', end_date='20220430'):
    # Get all files in the directory
    files = os.listdir(DATA_PATH)
    # Convert date strings to datetime objects for comparison
    try:
        start = datetime.strptime(start_date, '%Y%m%d')
        end = datetime.strptime(end_date, '%Y%m%d')
    except ValueError as e:
        log.error(f"Invalid date format: {e}")
        return []
    # Filter files based on date range
    filtered_files = []
    for file in files:
        # Extract date from filename (assuming format YYYYMMDD.json)
        try:
            file_date_str = file.split('.')[0]
            file_date = datetime.strptime(file_date_str, '%Y%m%d')
            # Check if file date is within the specified range
            if start <= file_date <= end:
                filtered_files.append(file)
        except ValueError:
            continue  # Skip files with invalid date format
    
    # Sort files by date
    filtered_files.sort()
    return filtered_files

def readData(file):
	log.info("--------------READING DATA-------------")
	df = spark.read.json(DATA_PATH+file)
	log.info("---------------------------------------")
	return df.select("_source.*")

def transformingData(df):
	log.info("-----------TRANSFORMING DATA-----------")
	df = df.withColumn("Type",
           when((col("AppName") == 'CHANNEL') | (col("AppName") =='DSHD')| (col("AppName") =='KPLUS')| (col("AppName") =='KPlus'), "Truyền Hình")
          .when((col("AppName") == 'VOD') | (col("AppName") =='FIMS_RES')| (col("AppName") =='BHD_RES')| 
                 (col("AppName") =='VOD_RES')| (col("AppName") =='FIMS')| (col("AppName") =='BHD')| (col("AppName") =='DANET'), "Phim Truyện")
          .when((col("AppName") == 'RELAX'), "Giải Trí")
          .when((col("AppName") == 'CHILD'), "Thiếu Nhi")
          .when((col("AppName") == 'SPORT'), "Thể Thao")
          .otherwise("Error"))
	# df.show(5)
	df = df.filter(df.Contract != '0')
	df = df.filter(df.Type != 'Error')

	windowspec = Window.partitionBy("Contract")
	df = df.withColumn("Active", sf.count("Date").over(windowspec))
	df = df.drop("Date")
	df = df.withColumn("Active",when(col("Active") > 20, "High")
		.when((col("Active") <= 20) & (col("Active") > 10), "Medium")
		.otherwise("Low")
	)
	# df.show(5)
	result = df.groupBy('Contract','Active').pivot('Type').sum()\
	.withColumnRenamed('Giải Trí','GiaiTri')\
	.withColumnRenamed('Phim Truyện','PhimTruyen')\
	.withColumnRenamed('Thiếu Nhi','ThieuNhi')\
	.withColumnRenamed('Thể Thao','TheThao')\
	.withColumnRenamed('Truyền Hình','TruyenHinh')
	# result.show(5)
	#Tạo thêm cột MostWatch, tính toán xem ng dùng xem Type nào nhiều nhất, sử dụng hàm greatest(col1,col2,col3,...)
	result = result.withColumn("MostWatch",greatest(col("GiaiTri"),col("PhimTruyen"),col("TheThao"),col("ThieuNhi"),col("TruyenHinh")))
	#Sau khi tạo cột MostWatch bây giờ giá trị của cột sẽ bằng đúng giá trị lớn nhất trong các cột được xét như ở trên-> sử dụng case when để biến đổi thành tên cột thay vì giá trị bản ghi
	result = result.withColumn("MostWatch",
	        when(col("MostWatch")==col("TruyenHinh"),"TruyenHinh")
	        .when(col("MostWatch")==col("PhimTruyen"),"PhimTruyen")
	        .when(col("MostWatch")==col("TheThao"),"TheThao")
	        .when(col("MostWatch")==col("ThieuNhi"),"ThieuNhi")
	        .when(col("MostWatch")==col("GiaiTri"),"GiaiTri"))
	# result.show(5)
	result = result.withColumn("CustomerTaste",
	concat_ws("-",
	                            when(col("GiaiTri").isNotNull(),lit("GiaiTri"))
	                            ,when(col("PhimTruyen").isNotNull(),lit("PhimTruyen"))
	                            ,when(col("TheThao").isNotNull(),lit("TheThao"))
	                            ,when(col("ThieuNhi").isNotNull(),lit("ThieuNhi"))
	                            ,when(col("TruyenHinh").isNotNull(),lit("TruyenHinh")))
	)
	# result.show(5)	
	log.info("----------------------------------------")
	return result

def saveResult(df_final,where):
	log.info("--------------SAVING RESULT-------------")
	df_final.repartition(1).write.mode("overwrite") \
            .option("header", "true") \
            .option("encoding", "UTF-8").csv(f"{OUTPUT_PATH}{where}",header=True)
	log.info("---------------------------------------")

def saveToDB(df_final):
	log.info("-----------SAVING TO DATABASE-----------")
	target_table = f"data_final"
	connection_properties = {
	    **tar_conn.T_CONN_PROPERTIES,
	    "batchsize": "10000",
	    "autoCommit": "true"
	}
	df_final.write.jdbc(
	url=tar_conn.T_JDBC_URL,
	table=f"{tar_conn.T_SCHEMA_NAME}.{target_table}",
	mode="overwrite",
	properties=connection_properties
	)
	log.info("---------------------------------------")
def main():
	# Get list of files to process
	start_date = '20220401'
	end_date = '20220430'
	files = getFilesToProcess(start_date, end_date)
	start_time = datetime.now()

	union_result = None 
	for file in files:
		df = readData(file)
		day = file.split(".")[0]
		df = df.select("Contract","AppName","TotalDuration")\
			.withColumn("Date",lit(f"{day}"))

		# df.show(1)
		if union_result == None:
			union_result = df
		else:
			union_result = union_result.union(df)

	result = transformingData(union_result)
	# result.show(5)
	# saveResult(result,'method2')
	saveToDB(result)

	end_time = datetime.now()
	time_processing = (end_time - start_time).total_seconds()
	log.info(f"It took {time_processing} seconds to process all the data")

if __name__ == '__main__':
    try:
        main()
    except Exception as e:
        log.error(f"Error while running ETL process: {e}")
    else:
        log.info(f"ETL process ran successfully!")
    finally:
        log.warning("Closing application.....Done! \n\n")