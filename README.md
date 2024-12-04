
# Big Data Analysis of Electronics Reviews

This project analyzes Amazon electronics reviews to uncover insights into consumer sentiment using data processing and natural language processing techniques. By employing PySpark and Hadoop, the data is preprocessed, and word frequencies and TF-IDF scores are calculted.Ultimately, findings are visualized through word clouds. The objective is to identify key terms that influence customer satisfaction and dissatisfaction, aiding manufacturers and retailers in improving their products and strategies.

## Text Preprocessing with PySpark

### Overview

Python script (`preprocess.py`) for preprocessing text data from a JSON file (`Some_Input_File.json`) using **PySpark**. The preprocessing includes:

- **Text Cleaning:** Lowercasing, tokenization, removing stopwords and non-alphabetic characters.
- **Stemming and Lemmatization:** Reducing words to their base or root form.
- **Dataset Balancing:** Ensuring an equal number of positive and negative reviews.
- **Output Generation:** Saving the processed data as CSV files.

### Table of Contents

1. Prerequisites
2. Installation
    - 2.1 Install Java 11
    - 2.2 Set Up Python Virtual Environment
    - 2.3 Install Required Python Packages
3. Usage
    - 3.1 Running the Script
    - 3.2 Script Arguments
4. Output
5. Logging

---

### Prerequisites

Before setting up and running the script, ensure that your system meets the following requirements:

- **Operating System:** Windows, macOS, or Linux
- **Python Version:** Python 3.9
- **Java Version:** Java 8 or Java 11

*Note:* PySpark is compatible with Java 8 and Java 11. Ensure that you install one of these versions to avoid compatibility issues.

### Installation

Follow the steps below to set up the environment and install all necessary dependencies.

#### 2.1 Install Java 11

PySpark requires a Java Runtime Environment (JRE) to function correctly. Ensure you have either Java 8 or Java 11 installed.


#### 2.2 Set Up Python Virtual Environment

1. Install `venv` (if not already installed):

   `venv` is included with Python 3.9, but ensure it's available:

   ```bash
   python3 -m ensurepip --upgrade
   ```

2. Create a Virtual Environment:

   Navigate to your project directory and create a virtual environment named `env`:

   ```bash
   python3 -m venv env
   ```

3. Activate the Virtual Environment:

   - **macOS/Linux:**

     ```bash
     source env/bin/activate
     ```

   - **Windows:**

     ```bash
     env\Scripts\activate
     ```


#### 2.3 Install Required Python Packages

1. Install Dependencies:

   ```bash
   pip install -r requirements.txt
   ```

### Usage

#### 3.1 Running the Script

Execute the `preprocess.py` script.

```bash
python preprocess.py --input Your_Input_File.json --output_dir .
```

#### 3.2 Script Arguments

The script accepts the following command-line arguments:

- `--input`: Path to the input JSON file. 
- `--output_dir`: *(Optional)* Directory where the output CSV files will be saved. Defaults to the current directory `'.'`.
- `--stem`: *(Optional)* If included, applies stemming to the text.
- `--lemmatize`: *(Optional)* If included, applies lemmatization to the text.

**Examples:**

- **Apply Both Stemming and Lemmatization:**

  ```bash
  python preprocess.py --input Apple_Products.json --output_dir ./output --stem --lemmatize
  ```

- **Apply Only Stemming:**

  ```bash
  python preprocess.py --input Apple_Products.json --output_dir ./output --stem
  ```

- **Apply Only Lemmatization:**

  ```bash
  python preprocess.py --input Apple_Products.json --output_dir ./output --lemmatize
  ```

- **Apply Neither (Only Cleaning):**

  ```bash
  python preprocess.py --input Apple_Products.json --output_dir ./output
  ```

### Output

After successful execution, the specified `output_dir` will contain one or more of the following CSV files depending on the arguments provided:

- `balanced_cleaned.csv`: Cleaned text without stemming or lemmatization.
- `balanced_stemmed.csv`: Cleaned and stemmed text.
- `balanced_lemmatized.csv`: Cleaned and lemmatized text.
- `balanced_all.csv`: Cleaned, stemmed, and lemmatized text.

**Note:** Each CSV file includes:

- `overall`: The rating score (used for balancing the dataset).
- `cleaned_reviewText`: The cleaned text.
- `cleaned_reviewText_stemmed`: *(Optional)* The stemmed text.
- `cleaned_reviewText_lemmatized`: *(Optional)* The lemmatized text.

Additionally, two log files are generated in the project directory:

- `app.log`: Contains informational messages about the script's execution.
- `error.log`: Captures any errors encountered during processing.

### Logging

#### Reviewing Log Files

- **`app.log`**

- **`error.log`**


**Example:**

```bash
cat app.log
cat error.log
```


## Hadoop File System Preparation

Create the necessary directories in HDFS, make sure you already started all Hadoop daemons, the namenode, datanodes by `start-all.sh` first as we learn on Labs.

For example for lemmatization we can do it this way

```
hdfs dfs -mkdir /Reviews_Lemma

hdfs dfs -mkdir /Reviews_Lemma/Input

hdfs dfs -mkdir /Reviews_Lemma/Output
```

### Hadoop MapReduce Implementation

Put the cleaned data into HDFS using the following commands:

For the dataset with lemmatization:

```
hdfs dfs -put ./positive_reviews_lemmatized.csv /Reviews_Lemma/Input
hdfs dfs -put ./negative_reviews_lemmatized.csv /Reviews_Lemma/Input
```

### Running the TF-IDF calculation process

MapReduce.java contains the implementation of the TF-IDF calculation. Compile and run the code using the following commands:
```
javac -classpath $(hadoop classpath) -d MapReduce_classes MapReduce.java
jar -cvf MapReduce.jar -C MapReduce_classes/ .
hadoop jar MapReduce.jar MapReduce /Reviews/Input/positive_reviews_lemmatized.csv /Reviews/Output/positive/
hadoop jar MapReduce.jar MapReduce /Reviews/Input/negative_reviews_lemmatized.csv /Reviews/Output/negative/
```

Repeat the hadoop jar command as necessary for the other datasets.

### Retrieving Output Data

After completing the MapReduce jobs, merge and retrieve the output job files with the following commands:

```
hdfs dfs -getmerge /Reviews/Output/positive/wordcount ./wordcount_positive.txt
hdfs dfs -getmerge /Reviews/Output/negative/wordcount ./wordcount_negative.txt
hdfs dfs -getmerge /Reviews/Output/positive/tfidf ./tfidf_positive.txt
hdfs dfs -getmerge /Reviews/Output/negative/tfidf ./tfidf_negative.txt
```



