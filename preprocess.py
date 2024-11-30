import argparse
import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf, lower, regexp_extract, split, size, col
from pyspark.sql.types import StringType, StructType, StructField, FloatType
import nltk
from nltk.corpus import stopwords
from nltk.stem import PorterStemmer, WordNetLemmatizer
import sys
import re

nltk.download('punkt_tab')
nltk.download('stopwords')
nltk.download('wordnet')

# Setup Logging
app_logger = logging.getLogger('app_logger')
app_logger.setLevel(logging.INFO)
app_handler = logging.FileHandler('app.log')
app_handler.setLevel(logging.INFO)
app_logger.addHandler(app_handler)

error_logger = logging.getLogger('error_logger')
error_logger.setLevel(logging.ERROR)
error_handler = logging.FileHandler('error.log')
error_handler.setLevel(logging.ERROR)
error_logger.addHandler(error_handler)

def main():
    parser = argparse.ArgumentParser(description='Preprocess text data with PySpark')
    parser.add_argument('--input', type=str, default='Apple_Products.json', help='Input JSON file path')
    parser.add_argument('--output_dir', type=str, default='data', help='Output directory path')
    parser.add_argument('--stem', action='store_true', help='Apply stemming')
    parser.add_argument('--lemmatize', action='store_true', help='Apply lemmatization')
    args = parser.parse_args()

    try:
        spark = SparkSession.builder \
            .appName('TextPreprocessing') \
            .config("spark.ui.showConsoleProgress", "false") \
            .config("spark.executor.extraJavaOptions", "-Djava.security.auth.login.config=/dev/null") \
            .config("spark.driver.extraJavaOptions", "-Djava.security.auth.login.config=/dev/null") \
            .getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        app_logger.info('Spark session started')

        schema = StructType([
            StructField("reviewText", StringType(), True),
            StructField("overall", FloatType(), True)
        ])

        data = spark.read.json(args.input, schema=schema, multiLine=False)
        app_logger.info(f'Data loaded from {args.input}')

        data = data.select('reviewText', 'overall').na.drop()
        app_logger.info('Selected relevant columns and dropped nulls')

        stop_words = set(stopwords.words('english'))
        stemmer = PorterStemmer()
        lemmatizer = WordNetLemmatizer()

        def process_text(text):
            try:
                text = text.lower()
                words = nltk.word_tokenize(text)
                words = [word for word in words if word.isalpha() and word not in stop_words]
                cleaned = ' '.join(words)
                stemmed = ' '.join([stemmer.stem(word) for word in words])
                lemmatized = ' '.join([lemmatizer.lemmatize(word) for word in words])
                return (cleaned, stemmed, lemmatized)
            except Exception as e:
                error_logger.error(f'Error processing text: {e}')
                return (None, None, None)

        process_udf = udf(process_text, StructType([
            StructField("cleaned", StringType(), True),
            StructField("stemmed", StringType(), True),
            StructField("lemmatized", StringType(), True)
        ]))

        data = data.withColumn('processed', process_udf(col('reviewText')))
        app_logger.info('Applied text processing UDF')

        data = data.select(
            'overall',
            col('processed.cleaned').alias('cleaned_reviewText'),
            col('processed.stemmed').alias('cleaned_reviewText_stemmed'),
            col('processed.lemmatized').alias('cleaned_reviewText_lemmatized')
        )

        if not args.stem:
            data = data.drop('cleaned_reviewText_stemmed')
        if not args.lemmatize:
            data = data.drop('cleaned_reviewText_lemmatized')

        app_logger.info('Selected processed columns based on arguments')

        data = data.filter(size(split(col('cleaned_reviewText'), ' ')) > 0)
        if args.stem:
            data = data.filter(size(split(col('cleaned_reviewText_stemmed'), ' ')) > 0)
        if args.lemmatize:
            data = data.filter(size(split(col('cleaned_reviewText_lemmatized'), ' ')) > 0)

        app_logger.info('Filtered records with more than one word')

        negative = data.filter(col('overall').isin([1.0, 2.0]))
        positive = data.filter(col('overall') == 5.0)

        count_neg = negative.count()
        count_pos = positive.count()
        app_logger.info(f'Negative reviews count: {count_neg}, Positive reviews count: {count_pos}')

        if count_pos > 0:
            sampled_positive = positive.sample(False, count_neg / count_pos, seed=123)
            balanced = negative.union(sampled_positive)
            balanced = balanced.orderBy('overall')
            app_logger.info('Created balanced dataset')
        else:
            balanced = negative
            app_logger.warning('No positive reviews found to sample.')

        # Define the output based on arguments
        if args.stem and args.lemmatize:
            balanced.select('overall', 'cleaned_reviewText', 'cleaned_reviewText_stemmed', 'cleaned_reviewText_lemmatized') \
                    .write.csv(f"{args.output_dir}/balanced_all.csv", header=True, mode='overwrite')
            app_logger.info('Saved balanced dataset with all preprocessings')
        elif args.stem:
            positive.select("cleaned_reviewText_stemmed").alias("reviewText") \
                    .write.csv(f"{args.output_dir}/positive_reviews_stemmed.csv", header=True, mode='overwrite')
            negative.select("cleaned_reviewText_stemmed").alias("reviewText") \
                    .write.csv(f"{args.output_dir}/negative_reviews_stemmed.csv", header=True, mode='overwrite')
            app_logger.info('Saved positive and negative reviews with stemming')
        elif args.lemmatize:
            positive.select("cleaned_reviewText_lemmatized").alias("reviewText") \
                    .write.csv(f"{args.output_dir}/positive_reviews_lemmatized.csv", header=True, mode='overwrite')
            negative.select("cleaned_reviewText_stemmed").alias("reviewText") \
                    .write.csv(f"{args.output_dir}/negative_reviews_lemmatized.csv", header=True, mode='overwrite')
            app_logger.info('Saved positive and negative reviews with lemmatization')
        else:
            balanced.select('overall', 'cleaned_reviewText') \
                    .write.csv(f"{args.output_dir}/balanced_cleaned.csv", header=True, mode='overwrite')
            app_logger.info('Saved balanced cleaned dataset')

        spark.stop()
        app_logger.info('Spark session stopped')
    except Exception as e:
        error_logger.error(f'Error in main execution: {e}')
        sys.exit(1)

if __name__ == '__main__':
    main()
