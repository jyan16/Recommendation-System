from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession, SQLContext, Row
from pyspark.sql.types import *
import pymongo as mg
import argparse

RECOMMEND_LENGTH = 3

##### util ##########################################################################################
def parse_args():
    parser = argparse.ArgumentParser(description='test cloud setup')
    parser.add_argument('-mongo', help='MongoDB database URI')
    parser.add_argument('-b', help='broker list', default='recommend-w-0:9092')
    return parser.parse_args()

def getSparkSessionInstance(sparkConf):
    if ('sparkSessionSingletonInstance' not in globals()):
        globals()['sparkSessionSingletonInstance'] = SparkSession \
            .builder \
            .appName("data1030") \
            .config("spark.mongodb.input.uri", args.mongo) \
            .config("spark.mongodb.output.uri", args.mongo) \
            .getOrCreate()

    return globals()['sparkSessionSingletonInstance']

#####################################################################################################
def mapper1(record):
    """
    :param record: "user_id,movie_id,rating,timestamp"
    :return: (key, value)
              key: user_id
              value: movie_id
    """
    mylist = record[1].split(",")
    return (int(mylist[0]), int(mylist[1]))

def mapper2(record):
    """
    process previous recommend list
    :param record: (13, [3, 4])
    :return:
    """
    key = record[0]
    result = []
    for item in record[1]:
        result.append((key, item))
    return result

def mapper3(record):
    """

    :param record: (user_id, movie_list)
    :return: [(user_id, movie_list), ...]
    """
    key = record[0]
    result = []
    for movie in record[1]:
        result.append((key, movie))
    return result

def mapper4(record):
    """
    :param record: Row(user_id=13, movie_id=7153, rating_avg=4.127840909090909, title='Lord of the Rings: The Return of the King, The (2003)')
    :return: (key, value)
              key: user_id,
              value: (movie_id, [(rating_avg, title)])
    """
    record = list(record)
    key = record[0]
    value = tuple(record[1:])

    return (key, [value])

def mapper5(record):
    """
    sort
    :param record:(15, [(1580, 3.663157894736842, 'Men in Black (a.k.a. MIB) (1997)'), (1291, 4.017006802721088, 'Indiana Jones and the Last Crusade (1989)'), (1270, 4.015486725663717, 'Back to the Future (1985)')])
    :return: sorted record
    """
    key = record[0]
    record[1].sort(key=lambda x: x[1],reverse=True)

    return (key, record[1])

def mapper6(record):
    """
    :param record:Row(user_id=503, sort_list=[Row(_1=1291, _2=4.017006802721088, _3='Indiana Jones and the Last Crusade (1989)'), Row(_1=1270, _2=4.015486725663717, _3='Back to the Future (1985)'), Row(_1=1580, _2=3.663157894736842, _3='Men in Black (a.k.a. MIB) (1997)')], movie_list=[1210])
    :return:(503, [(1291, 'Indiana Jones and the Last Crusade (1989)'), (1270, 'Back to the Future (1985)'), (1580, 'Men in Black (a.k.a. MIB) (1997)')])
    """
    movie_seen = record.movie_list
    user_id = record.user_id
    recommend_list = []
    count = 0
    for movie in record.sort_list:
        if movie[0] not in movie_seen:
            tmp = (movie[0], movie[2])
            recommend_list.append(tmp)
            count += 1
        if count == RECOMMEND_LENGTH:
            break
    return (user_id, recommend_list)

def mapper7(record):
    """
    process current recommend list
    :param record: Row(user_id=503, _id=Row(oid='599c2f83f787c8102d460aab'), item_1=Row(_1=1580, _2=0.45357142857142857), item_2=Row(_1=1270, _2=0.4524590163934426), item_3=Row(_1=1291, _2=0.4444444444444444), popular_id=1210)
    :return:
    """
    record = list(record)
    key = record[0]
    result = []
    for item in record:
        if type(item) == Row and len(item) == 2:
            result.append((key, [item[0]]))

    return result

def mapper8(record):
    """
    deduplicate
    :param record: (user_id, [movie_id, ...])
    :return:
    """
    movie_list = list(set(record[1]))
    key = record[0]
    result = []
    for movie in movie_list:
        result.append((key, movie))

    return result

def mapper9(record):
    """

    :param record: (13, [(5952, 'Lord of the Rings: The Two Towers, The (2002)'), (6539, 'Pirates of the Caribbean: The Curse of the Black Pearl (2003)')])
    :return:
    """
    key = record[0]
    result = []
    for movie in record[1]:
        result.append(movie[0])
    return (key, result)

def reducer(a, b):
    return a + b

def process(rdd):
    """
    :param rdd: (user_id, movie_id)
    :return: (user_id, [movie_id, ...])
    """
    if rdd.isEmpty():
        return rdd

    spark = getSparkSessionInstance(rdd.context.getConf())
    ssc = SQLContext(spark.sparkContext)

    # 80
    # update recommend table with newest; create dataframe of recommend table
    tmp = rdd.collect()
    for item in tmp:
        collect.update_one(
            {"user_id": item[0]},
            {"$addToSet": {"movie_list": item[1]}}
        )

    user_list = rdd.map(lambda x: [x[0]]).reduce(reducer)
    tmp_list = []
    for item in collect.find({'user_id':{'$in':user_list}}):
        tmp_list.append((item['user_id'], item['movie_list']))
    tmp_df = ssc.createDataFrame(tmp_list, ['user_id', 'movie_list'])
    ssc.registerDataFrameAsTable(tmp_df, 'recommend')

    # 82: get the whole recommend list of incoming user
    input_rdd = sc.parallelize(tmp_list).flatMap(mapper3)
    input_df = spark.createDataFrame(input_rdd, ['user_id', 'movie_id'])
    ssc.registerDataFrameAsTable(input_df, 'input_rating')

    current_rdd = ssc.sql('''SELECT i.user_id, s.*
                        FROM input_rating i, similar s
                        WHERE i.movie_id == s.popular_id ''').rdd

    # 86
    total_rdd = current_rdd.flatMap(mapper7)\
        .reduceByKey(reducer)\
        .flatMap(mapper8)

    # 88: sort the recommend list with avg_rating
    recommend_df = spark.createDataFrame(total_rdd, ['user_id', 'movie_id'])
    ssc.registerDataFrameAsTable(recommend_df, 'user_new')
    movie_rating_df = ssc.sql('''SELECT u.user_id, u.movie_id, m.rating_avg, m.title
                                 FROM user_new u, movies m
                                 WHERE u.movie_id == m.movie_id''')
    movie_rating_rdd = movie_rating_df.rdd\
        .map(tuple)\
        .map(mapper4)\
        .reduceByKey(reducer)\
        .map(mapper5)

    # 90: filter out recommend items that are already in table recommend.movie_list
    #     truncate to RECOMMEND_LENGTH
    movie_rating_df2 = ssc.createDataFrame(movie_rating_rdd, ['user_id', 'sort_list'])
    ssc.registerDataFrameAsTable(movie_rating_df2, 'sorted')
    movie_list_rdd = ssc.sql('''SELECT s.user_id, s.sort_list, r.movie_list
                                FROM sorted s, recommend r
                                WHERE s.user_id == r.user_id''').rdd
    filtered_movie_rdd = movie_list_rdd.map(mapper6)
    recommend_list = filtered_movie_rdd.map(mapper9).collect()

    # 92
    for item in recommend_list:
        collect.update_one(
            {"user_id": item[0]},
            {"$set": {"recommend_list": item[1]}}
        )

    # clean up temporary tables
    ssc.dropTempTable('input_rating')
    ssc.dropTempTable('user_new')
    ssc.dropTempTable('recommend')
    ssc.dropTempTable('sorted')

    return filtered_movie_rdd

if __name__ == '__main__':

    args = parse_args()

    # pre 80
    # init spark streaming with batch interval of 5 seconds
    spark = SparkSession \
        .builder \
        .appName("data1030") \
        .config("spark.mongodb.input.uri", args.mongo) \
        .config("spark.mongodb.output.uri", args.mongo) \
        .getOrCreate()
    sc = spark.sparkContext
    stream = StreamingContext(sc, 5)
    ssc = SQLContext(sc)
    client = mg.MongoClient(args.mongo)
    collect = client.netflix.recommend

    # load existing table from MongoDB
    similar_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "similar").load()
    ssc.registerDataFrameAsTable(similar_df, 'similar')
    movies_df = spark.read.format("com.mongodb.spark.sql.DefaultSource").option("collection", "movies").load()
    ssc.registerDataFrameAsTable(movies_df, 'movies')

    # initialize recommend table in MongoDB
    schema = StructType([
        StructField("user_id", IntegerType(), False),
        StructField("movie_list", ArrayType(IntegerType()), True),
        StructField("recommend_list", ArrayType(IntegerType()), True)
    ])

    test_list = [(i, [], []) for i in range(20)]
    test_rdd = sc.parallelize(test_list)
    recommend_df = ssc.createDataFrame(test_rdd, schema)

    recommend_df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .option("collection", "recommend") \
        .mode("overwrite").save()

    ##### streaming manipulation #########################################################################
    kafka_stream = KafkaUtils.createDirectStream(stream, ['ratings'], {"metadata.broker.list": args.b})

    calculate_rdd = kafka_stream.map(mapper1).transform(process)
    calculate_rdd.pprint(20)

    stream.start()
    stream.awaitTermination()

