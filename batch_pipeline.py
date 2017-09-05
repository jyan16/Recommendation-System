from pyspark.sql import SparkSession, SQLContext
import argparse
POPULAR_THRESHOLD = 200
RATING_NUM = 3
n = 3

##### util ##########################################################################################
def parse_args():
    parser = argparse.ArgumentParser(description='test cloud setup')
    parser.add_argument('-mongo', help='MongoDB database URI')
    parser.add_argument('-r', help='path to input ratings.csv', default='../data/ratings.csv')
    parser.add_argument('-m', help='path to input movies.csv', default='../data/movies.csv')
    return parser.parse_args()

def jaccard_similarity(n_common, n1, n2):
    # http://en.wikipedia.org/wiki/Jaccard_index
    numerator = n_common
    denominator = n1 + n2 - n_common
    if denominator == 0:
        return 0.0
    return numerator / denominator

def combinations(iterable, r):
    # http://docs.python.org/2/library/itertools.html#itertools.combinations
    # combinations('ABCD', 2) --> AB AC AD BC BD CD
    # combinations(range(4), 3) --> 012 013 023 123
    pool = tuple(iterable)
    n = len(pool)
    if r > n:
        return
    indices = list(range(r))
    yield tuple(pool[i] for i in indices)
    while True:
        for i in reversed(list(range(r))):
            if indices[i] != i + n - r:
                break
        else:
            return
        indices[i] += 1
        for j in range(i+1, r):
            indices[j] = indices[j-1] + 1
        yield tuple(pool[i] for i in indices)

### mapreduce #########################################################################################


def mapper2(record):
    """
    :param record: "user_id, movie_id, rating, timestamp"
    :return: (key, value)
              key: movie_id
              value: [(user_id, rating)]
    """
    key = record[1]
    value = [(record[0], record[2])]
    return (key, value)

def mapper3(record):
    """
    :param record: (key, value)
                    key: movie_id
                    value: [(user_id, rating), ...]
    :return: [(movie_id, )] or []
    """
    key= record[0]
    value = len(record[1])
    return [(key)] if value >= POPULAR_THRESHOLD else []

def mapper4(record):
    """
    :param record: (movie_id, [(user_id, rating), ...])
    :return: [(user_id, [(movie_id, rating, num_rater)]), ...]
    """
    result = []
    num_rater = len(record[1])
    for item in record[1]:
        tmp = (item[0], [(record[0], item[1], num_rater)])
        result.append(tmp)
    return result

def mapper5(record):
    """
    to generate tuple that rated by the same user
    :param record: (user_id, [(movie_id, rating, num_rater), ...])
    :return: [( ( movie_id1, movie_id2 ), [(num_rater1, num_rater2)] ), ...]
    """
    result = []
    record[1].sort(key=lambda item:item[0])
    pair = combinations(record[1],2)
    for item in pair:
        if item[0][0] in popular_item:
            tmp = ((item[0][0], item[1][0]), [(item[0][2], item[1][2])])
            result.append(tmp)
    return result

def mapper6(record):
    """
    :param record: (key, value)
                     key: (movie_id1, movie_id2)
                     value: [(num_rater1, num_rater2), ...]
    :return: [(key, value)] or []
               key: movie_id
               value: [(movie_id2, jaccard)]
    """

    n1 = record[1][0][0]
    n2 = record[1][0][1]
    n = len(record[1])

    jaccard_value = jaccard_similarity(n, n1, n2)
    result = []
    if n >= RATING_NUM:
        tmp = (record[0][0], [(record[0][1], jaccard_value)])
        result.append(tmp)
    return result

def mapper7(record):
    """
    :param record: (key, value)
                    key: movie_id1
                    value: [(movie_id2, jaccard), ...]
    :return: (key, value)
              key: movie_id1
              value: top n item
    """
    record[1].sort(key=lambda x: x[1], reverse=True)
    tmp = record[1][0:n]
    result = [tuple([record[0]] + tmp)]
    return result

def reducer(a, b):
    return a + b

if __name__ == '__main__':

    args = parse_args()

    # spark init
    spark = SparkSession \
        .builder \
        .appName("data1030") \
        .config("spark.mongodb.input.uri", args.mongo) \
        .config("spark.mongodb.output.uri", args.mongo) \
        .getOrCreate()
    sc = spark.sparkContext
    ssc = SQLContext(sc)

    # 100
    rating_df = ssc.read.format('csv') \
        .option('header', 'true') \
        .option('inferschema', 'true').option('mode', 'DROPMALFORMED').load(args.r)
    movie_df = ssc.read.format('csv') \
        .option('header', 'true') \
        .option('inferschema', 'true').option('mode', 'DROPMALFORMED').load(args.m)

    ssc.registerDataFrameAsTable(rating_df, 'ratings')
    ssc.registerDataFrameAsTable(movie_df, 'movies')

    tmp_df = ssc.sql('''SELECT r.movieId, r.rating, m.title
                        FROM ratings r, movies m
                        WHERE r.movieId = m.movieId''')
    ssc.registerDataFrameAsTable(tmp_df, 'tmp_table')

    table_2_df = ssc.sql('''SELECT movieId AS movie_id, AVG(rating) AS rating_avg, title
                            FROM tmp_table
                            GROUP BY movieId, title''')

    table_2_df.write.format("com.mongodb.spark.sql.DefaultSource")\
        .option("collection", "movies")\
        .mode("overwrite").save()

    # 104
    rdd = rating_df.rdd.map(tuple)
    item_user = rdd.map(mapper2).reduceByKey(reducer)

    # 106
    popular_item = item_user.map(mapper3).reduce(reducer)

    # 108, 110, 112, 114, 116
    similar_table = item_user.flatMap(mapper4) \
        .reduceByKey(reducer) \
        .flatMap(mapper5) \
        .reduceByKey(reducer) \
        .flatMap(mapper6) \
        .reduceByKey(reducer) \
        .flatMap(mapper7)

    # load table into MongoDB
    column_list = ['popular_id']
    for i in range(1, n + 1):
        column_list.append('item_'+str(i))
    similar_table_df = spark.createDataFrame(similar_table, column_list)

    similar_table_df.write.format("com.mongodb.spark.sql.DefaultSource") \
        .option("collection", "similar") \
        .mode("overwrite").save()




