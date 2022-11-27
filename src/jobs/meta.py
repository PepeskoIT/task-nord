from pyspark import SparkContext as sc
# from pyspark.streaming import StreamingContext

# import urllib.request
# import shutil

# S3_MALWARE_DIR = "1"
# S3_CLEAN_DIR = "0"

# def downloader(url, outpath):
#     # From URL construct the destination path and filename.
#     file_name = os.path.basename(urllib.parse.urlparse(url).path)
#     file_path = os.path.join(outpath, file_name)    # Check if the file has already been downloaded.
#     if os.path.exists(file_path):
#         return    # Download and write to file.
#     with urllib.request.urlopen(url, timeout=5) as urldata, open(file_path, ‘wb’) as out_file:
#         shutil.copyfileobj(urldata, out_file)




# # Create a local StreamingContext with two working thread and batch interval of 1 second
# sc = SparkContext("local[2]", "NetworkWordCount")
# ssc = StreamingContext(sc, 1)
from envs import S3_STORAGE_URL
from jobs.aquisition import S3Agregator

S3_AGREGATOR = S3Agregator(S3_STORAGE_URL)


def calculate_cnt_div(n):
    return 0, 2
    # return n//2, n//2

# def process_clean(n):
#     S3_AGREGATOR.process_clean_data(1)


# def process_malicious(n):
#     S3_AGREGATOR.process_malicious_data(1)


def process_all(n):
    malicious_cnt, clean_cnt = calculate_cnt_div(n)
    S3_AGREGATOR.process_data(malicious_cnt, clean_cnt)
