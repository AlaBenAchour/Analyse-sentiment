import org.apache.spark.ml.feature.{CountVectorizer, MinMaxScaler, StopWordsRemover, Tokenizer, VectorAssembler}
import scalaj.http._
import play.api.libs.json._
import org.apache.spark.sql.{ Row, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.ml.linalg.Vector



object AnaliticSentimentRealTime {

  object FacebookAPI {
    val accessToken = "EAAElY2QZCDk4BO8SV6dFbIOtfJgfZCMP1jj5JMBRtJrATQQ2SXRZBEoOdY4Q15KJAidd1A0uQ7odr8H3HYpleXjzJL0fRWN0YHWWSazGbhARaYT08Ag79A3mGNivKahEdDN0Ry8o9pdgcMRS9a2kjRZB681odZBe9MYOP1iE7wCNKL5HzcCGHkm84IoZCb491Bo1z0qaxRcdboUeDZAt2U63FFrrij2pIWZAhOZCWFOgMzlFY13MUZBgb5"

    def fetchData(path: String): String = {
      val response = Http(s"https://graph.facebook.com/$path").param("access_token", accessToken).asString
      response.body
    }
  }

  case class Post(id: String, createdTime: String, postNumber: Int, message: Option[String], description: Option[String], link: Option[String], totalCount: Int)




  def main(args: Array[String]): Unit = {
    // Create SparkSession
    val spark = SparkSession.builder()
      .appName("AnalyticSentimentRealTime")
      .master("local[*]")  // Use local mode for simplicity
      .getOrCreate()

    import spark.implicits._

    // Your existing code goes here
    val path = "me?fields=posts{id,created_time,message,description,link,comments.limit(0).summary(true)}"
    val responseData = FacebookAPI.fetchData(path)
    println(responseData)

    // Parse JSON response
    val json = Json.parse(responseData)
    val posts = (json \ "posts" \ "data").as[Seq[JsValue]]

    // Extract relevant fields and normalize data
    val normalizedData = posts.zipWithIndex.map { case (post, index) =>
      val id = (post \ "id").as[String]
      val createdTime = (post \ "created_time").as[String]
      val message = (post \ "message").asOpt[String]
      val description = (post \ "description").asOpt[String]
      val link = (post \ "link").asOpt[String]
      val totalCount = (post \ "comments" \ "summary" \ "total_count").asOpt[Int].getOrElse(0) // Default to 0 if total_count is not present
      Post(id, createdTime, index + 1, message, description, link, totalCount)
    }

    // Create DataFrame from normalized data
    val df = normalizedData.toDF()

    // Show DataFrame in a tabular format
    df.show(false)

    // Show total number of posts
    println(s"Number of rows: ${df.count()}")
    println(s"Number of columns: ${df.columns.length}")

    // Show the first 10 records of the DataFrame
    df.show(10)

    // Collect all records into an array
    val records = df.collect()

    // Iterate over the array and print each record
    records.foreach { row =>
      val id = if (row.isNullAt(row.fieldIndex("id"))) "No ID" else row.getAs[String]("id")
      val createdTime = if (row.isNullAt(row.fieldIndex("createdTime"))) "No created time" else row.getAs[String]("createdTime")
      val postNumber = if (row.isNullAt(row.fieldIndex("postNumber"))) -1 else row.getAs[Int]("postNumber")
      val message = if (row.isNullAt(row.fieldIndex("message"))) "No message" else row.getAs[String]("message")
      val description = if (row.isNullAt(row.fieldIndex("description"))) "No description" else row.getAs[String]("description")
      val link = if (row.isNullAt(row.fieldIndex("link"))) "No link" else row.getAs[String]("link")
      val totalCount = row.getAs[Int]("totalCount")
      println(s"Post ID: $id, Created Time: $createdTime, Post Number: $postNumber, Message: $message, Description: $description, Link: $link, Total Count: $totalCount")
    }

    // Show the number of partitions
    println(s"Number of partitions: ${df.rdd.getNumPartitions}")
    // Print the column names
    println("Column names:")
    df.columns.foreach(println)


    // Check for null values in each column
    val nullCounts = df.columns.map(colName => {
      val nullCount = df.filter(col(colName).isNull).count()
      (colName, nullCount)
    })

    // Print the number of null values in each column
    nullCounts.foreach { case (colName, count) =>
      println(s"Column: $colName, Null Count: $count")
    }

    // Filter the DataFrame to retain only null values in the 'message' column
    val nullMessageCount = df.filter($"message".isNull).count()

    // Print the number of null values in the 'message' column
    println(s"Column: message, Null Count: $nullMessageCount")

    // Filter the DataFrame to retain only rows where the 'message' column is null
    val nullMessageDF = df.filter($"message".isNull)

    // Show the filtered DataFrame
    nullMessageDF.show(false)


    // Print the shape of the dataset before dropping null records
    println(s"Shape of the dataset before dropping null records: (${df.count()}, ${df.columns.length})")

    // Drop null records
    val dfWithoutNull = df.na.drop()

    // Print the shape of the dataset after dropping null records
    println(s"Shape of the dataset after dropping null records: (${dfWithoutNull.count()}, ${dfWithoutNull.columns.length})")




    // Filter the DataFrame to get records where 'totalCount' is equal to 0
    val totalCountZeroDF = df.filter($"totalCount" === 0)

    // Count the total number of records where 'totalCount' is equal to 0
    val totalCountZero = totalCountZeroDF.count()

    // Print the total number of records where 'totalCount' is equal to 0
    println(s"Total number of records where totalCount is equal to 0: $totalCountZero")
    // Show the DataFrame where 'totalCount' is greater than 1
    totalCountZeroDF.show(false)




    val valueToCheck = 10 // Change this to the desired value

    // Filter the DataFrame to get records where 'totalCount' is equal to the specified value
    val totalCountEqualToValueDF = df.filter($"totalCount" === valueToCheck)

    // Count the total number of records where 'totalCount' is equal to the specified value
    val totalCountEqualToValue = totalCountEqualToValueDF.count()

    // Print the total number of records where 'totalCount' is equal to the specified value
    println(s"Total number of records where totalCount is equal to $valueToCheck: $totalCountEqualToValue")

    // Filter the DataFrame to get records where 'totalCount' is greater than 1
    val totalCountGreaterThan1DF = df.filter($"totalCount" > 1)

    // Count the total number of records where 'totalCount' is greater than 1
    val totalCountGreaterThan1 = totalCountGreaterThan1DF.count()

    // Print the total number of records where 'totalCount' is greater than 1
    println(s"Total number of records where totalCount is greater than 1: $totalCountGreaterThan1")
    // Show the DataFrame where 'totalCount' is greater than 1
    totalCountGreaterThan1DF.show(false)


    // Add a new column 'length' containing the length of strings in the 'description' column
    val dfWithLength = df.withColumn("length", length(coalesce($"description", lit(""))))

    // Show the DataFrame with the new 'length' column
    dfWithLength.show(false)

    // Extract original value of 'description', length of description, and 'length' column value for the 10th record
    val tenthRecord = dfWithLength.collect()(9)
    val originalDescription = tenthRecord.getAs[String]("description")
    val descriptionLength = if (originalDescription != null) originalDescription.length else 0
    val lengthColumnValue = tenthRecord.getAs[Int]("length")

    // Print the values
    println(s"Original Description: $originalDescription")
    println(s"Length of Description: $descriptionLength")
    println(s"'length' column value: $lengthColumnValue")


    // Find the maximum length of the 'description' column
    val maxLengthDF = dfWithLength.agg(max(length($"description")).as("max_length"))

    // Get the maximum length value
    val maxLength = maxLengthDF.first().getAs[Int]("max_length")

    // Filter the DataFrame to retrieve the row(s) with the maximum length
    val longestLinesDF = dfWithLength.filter(length($"description") === maxLength)

    // Print the longest line(s)
    println(s"Longest line(s):")
    longestLinesDF.show(false)

    // Get the data types of each column
    val columnDataTypes = dfWithLength.dtypes

    // Print the data types of each column
    println("Data types of each column:")
    columnDataTypes.foreach { case (columnName, columnType) =>
      println(s"$columnName: $columnType")
    }

    // Find the distinct values of 'totalCount' column and count the occurrences of each distinct value
    val distinctCountDF = df.groupBy("totalCount").count()

    // Print the distinct values of 'totalCount' and their counts
    println("Distinct values of 'totalCount' and their counts:")
    distinctCountDF.show(false)

    // Calculate the total number of 'totalCount'
    val totalCountTotal = df.selectExpr("sum(totalCount)").as[Long].first()

    // Print the total number of 'totalCount'
    println(s"Total number of 'totalCount': $totalCountTotal")

    // Group by 'totalCount' column and count the occurrences of each totalCount
    val totalCountOccurrencesDF = df.groupBy("totalCount").count()

    // Print the occurrences of each totalCount
    println("Occurrences of each totalCount:")
    totalCountOccurrencesDF.show(false)


    // Sort the DataFrame by 'totalCount' column in descending order
    val mostPostsDF = df.sort($"totalCount".desc)

    // Show the top records with the highest 'totalCount'
    println("Posts with the highest totalCount:")
    mostPostsDF.show(false)

    // Sort the DataFrame by 'totalCount' column in descending order and select the top 5
    val top5PostsDF = df.sort($"totalCount".desc).limit(5)

    // Show the top 5 posts with the highest 'totalCount'
    println("Top 5 posts with the highest totalCount:")
    top5PostsDF.show(false)


    // Base URL of the posts
    val baseURL = "https://www.facebook.com/"


    // Extract the IDs of the top 5 posts
    val top5IDs = top5PostsDF.select("id").collect().map(_.getString(0))

    // Convert IDs to links
    val top5Links = top5IDs.map(id => s"$baseURL$id")

    // Print the links of the top 5 posts with the highest 'totalCount'
    println("Links of the top 5 posts with the highest totalCount:")
    top5Links.foreach(println)


    // Group by "createdTime" column and count the occurrences of each unique value
    val countByCreatedTime = df.groupBy("createdTime").count()

    // Show the count of each unique value in the "createdTime" column
    println("Count of each unique value in the 'createdTime' column:")
    countByCreatedTime.show(false)


    // Extract the month from the "createdTime" column
    val dfWithMonth = df.withColumn("month", month($"createdTime"))

    // Group by "month" column and count the occurrences of each unique value
    val countByMonth = dfWithMonth.groupBy("month").count()

    // Show the count of each unique value in the "month" column
    println("Count of each unique value in the 'month' column:")
    countByMonth.show(false)

    // Extract the date component from the "createdTime" column
    val dfWithDate = df.withColumn("date", to_date($"createdTime"))

    // Group by "date" column and count the occurrences of each unique value
    val countByDate = dfWithDate.groupBy("date").count()

    // Show the total count of posts for each date
    println("Total count of posts for each date:")
    countByDate.show(false)



    // Calculate the percentage distribution of each 'totalCount'
    val percentageDistributionDF = df.groupBy("totalCount").count()
      .withColumn("percentage", col("count") * 100 / totalCountTotal)

    // Print the percentage distribution of each 'totalCount'
    println("Percentage distribution of each 'totalCount':")
    percentageDistributionDF.show(false)


    // Group by "message" column and count the occurrences of each unique value
    val countByMessage = df.groupBy("message").count()

    // Print the distinct values of 'message' and their counts
    println("Distinct values of 'message' and their counts:")
    countByMessage.show(false)

    // Group by "description" column and count the occurrences of each unique value
    val countByDescription = df.groupBy("description").count()

    // Print the distinct values of 'description' and their counts
    println("Distinct values of 'description' and their counts:")
    countByDescription.show(false)


    // Calculate the total count of all messages
    val totalMessageCount = df.select("message").na.drop.distinct.count()

    // Calculate the percentage distribution of each message
    val percentageDistributionMessage = df.groupBy("message").count()
      .withColumn("percentage", col("count") * 100 / totalMessageCount)

    // Print the percentage distribution of each message
    println("Percentage distribution of each message:")
    percentageDistributionMessage.show(false)

    // Calculate the total count of all descriptions
    val totalDescriptionCount = df.select("description").na.drop.distinct.count()

    // Calculate the percentage distribution of each description
    val percentageDistributionDescription = df.groupBy("description").count()
      .withColumn("percentage", col("count") * 100 / totalDescriptionCount)

    // Print the percentage distribution of each description
    println("Percentage distribution of each description:")
    percentageDistributionDescription.show(false)


    // Add a new column 'isFree' based on the presence of the word 'free' in the 'description' column
    val dfWithIsFree = df.withColumn("isFree", when(lower($"description").contains("free"), "Yes").otherwise("No"))

    // Show the DataFrame with the new 'isFree' column
    dfWithIsFree.select("description", "isFree").show(false)


    // Add a new column 'Existe Comments' based on the condition 'totalCount == 0'
    val dfWithExisteComments = df.withColumn("Existe Comments", when($"totalCount" === 0, false).otherwise(true))

    // Show the DataFrame with the new 'Existe Comments' column
    dfWithExisteComments.select("id", "Existe Comments").show(false)

    // Filter the DataFrame to select only the rows where 'Existe Comments' is true
    val filteredDF = dfWithExisteComments.filter($"Existe Comments" === true)


    // Convert the 'id' column to links
    val linkedDF = filteredDF.withColumn("Link", concat(lit(baseURL), $"id"))

    // Show the DataFrame with filtered rows and converted links
    linkedDF.select("id", "Link").show(false)

    // Compute statistics for the 'length' column
    val lengthStats = dfWithLength.select("length").describe()

    // Show the computed statistics
    lengthStats.show(false)



    // Filter out null values from the 'length' column
    val filteredLengthDF = dfWithLength.filter($"length".isNotNull)

    // Create a histogram of the non-null length values
    val histogram = filteredLengthDF.groupBy("length").count().sort("length").collect()

    // Print out the histogram bins and counts
    println("Histogram Bins and Counts:")
    histogram.foreach { row =>
      val bin = row.getAs[Int]("length")
      val count = row.getAs[Long]("count")
      println(s"Bin: $bin, Count: $count")
    }


    //  Add a new column 'messageLength' containing the length of strings in the 'message' column
    val dfWithMessageLength = df.withColumn("messageLength", length(coalesce($"message", lit(""))))

    //  Group the DataFrame by the 'messageLength' column
    val groupedDF = dfWithMessageLength.groupBy("messageLength")

    //  Calculate the mean message for each group
    val meanMessageDF = groupedDF.agg(mean("messageLength").as("meanMessageLength"))

    // Show the DataFrame with the mean message length for each group
    meanMessageDF.show(false)


    //  Add a new column 'descriptionLength' containing the length of strings in the 'description' column
    val dfWithDescriptionLength = df.withColumn("descriptionLength", length(coalesce($"description", lit(""))))

    // Group the DataFrame by the 'descriptionLength' column
    val groupedDFD = dfWithDescriptionLength.groupBy("descriptionLength")

    //  Calculate the mean description for each group
    val meanDescriptionDF = groupedDFD.agg(mean("descriptionLength").as("meanDescriptionLength"))

    // Show the DataFrame with the mean description length for each group
    meanDescriptionDF.show(false)


    // Filter out rows with empty or null values in the 'message' column
    val filteredDFD = df.filter($"message".isNotNull && $"message" =!= "")

    // Replace non-alphabet characters with a space in the 'message' column
    val replacedDF = filteredDFD.withColumn("cleaned_message", regexp_replace($"message", "[^a-zA-Zأ-ي\uD83D\uDE0A\uD83D\uDE03]", " "))

    // Convert the 'message' column to lower case
    val lowerCaseDF = replacedDF.withColumn("cleaned_message", lower($"cleaned_message"))

    // Split the 'message' column into words using Tokenizer
    val tokenizer = new Tokenizer().setInputCol("cleaned_message").setOutputCol("words")
    val tokenizedDF = tokenizer.transform(lowerCaseDF)

    // Show the resulting DataFrame
    tokenizedDF.select("words").show(false)



    // Filter out rows with empty or null values in the 'description' column
    val filteredD = df.filter($"description".isNotNull && $"description" =!= "")

    // Replace non-alphabet characters with a space in the 'description' column
    val replacedDD = filteredD.withColumn("cleaned_description", regexp_replace($"description", "[^a-zA-Zأ-ي\uD83D\uDE0A\uD83D\uDE03]", " "))

    // Convert the 'description' column to lower case
    val lowerCaseDD = replacedDD.withColumn("cleaned_description", lower($"cleaned_description"))

    // Split the 'description' column into words using Tokenizer
    val tokenizerD = new Tokenizer().setInputCol("cleaned_description").setOutputCol("words")
    val tokenizedDD = tokenizerD.transform(lowerCaseDD)
    tokenizedDD.select("words").show(false)





    // Remove stop words
    val remover = new StopWordsRemover().setInputCol("words").setOutputCol("filtered_words")
    val filteredDFF = remover.transform(tokenizedDD)

    // Build the corpus
    val corpusDF = filteredDFF.select("filtered_words")

    // Show the resulting corpus in a table format
    corpusDF.show(false)




    // Flatten the filtered words into a single column
    val flattenedDF = filteredDFF.select(explode($"filtered_words").alias("word"))

    // Count the frequency of each word
    val wordCountsDF = flattenedDF.groupBy("word").count()

    // Convert the DataFrame to Python to visualize the word cloud
    val wordCountsPython = wordCountsDF.collect().map { row =>
      (row.getString(0), row.getLong(1))
    }

    // Convert the word counts to JSON format
    val wordCountsJson = wordCountsPython.map { case (word, count) =>
      s"""{"text": "$word", "value": $count}"""
    }.mkString("[", ",", "]")

    // Show the unique words and their counts in a table format
    val wordCountsTable = wordCountsDF.orderBy($"count".desc)
    wordCountsTable.show(false)

    // Transfer the word counts JSON to a Python environment for visualization
    val wordCloudScript = s"""
import json
from wordcloud import WordCloud
import matplotlib.pyplot as plt

word_counts = $wordCountsJson
wordcloud = WordCloud(width=800, height=400, background_color='white').generate_from_frequencies(dict(word_counts))
plt.figure(figsize=(10, 8))
plt.imshow(wordcloud, interpolation='bilinear')
plt.axis('off')
plt.show()
"""

    // Execute the Python script to visualize the word cloud
    import scala.sys.process._
    Seq("python", "-c", wordCloudScript).!

    // Apply CountVectorizer to create a bag of words
    val countVectorizer = new CountVectorizer()
      .setInputCol("filtered_words")
      .setOutputCol("bag_of_words")
      .setVocabSize(1000) // Set the maximum vocabulary size

    // Fit and transform the DataFrame
    val bagOfWordsDF = countVectorizer.fit(filteredDFF).transform(filteredDFF)

    // Show the resulting DataFrame with the bag of words
    bagOfWordsDF.select("bag_of_words").show(false)


    // Print the shape of X
    println(s"Shape of X: (${bagOfWordsDF.count()}, ${bagOfWordsDF.select("bag_of_words").head().getAs[org.apache.spark.ml.linalg.SparseVector](0).size})")

    // Print the shape of y
    println(s"Shape of y: (${bagOfWordsDF.count()}, 1)")

    // Define the features column
    val assembler = new VectorAssembler()
      .setInputCols(Array("bag_of_words"))
      .setOutputCol("features")

    // Transform the DataFrame to include the features column
    val featureDF = assembler.transform(bagOfWordsDF)

    // Split the DataFrame into training and testing sets
    val Array(trainingData, testData) = featureDF.randomSplit(Array(0.8, 0.2), seed = 12345)

    // Optionally cache the DataFrames to optimize performance if needed
    trainingData.cache()
    testData.cache()

    // Show the shapes of the training and testing sets
    println(s"Training Data Shape: (${trainingData.count()}, ${trainingData.columns.length})")
    println(s"Testing Data Shape: (${testData.count()}, ${testData.columns.length})")


    // Check the available column names
    bagOfWordsDF.columns.foreach(println)

    // Split the bag of words DataFrame into training and testing sets
    val Array(trainDF, testDF) = bagOfWordsDF.randomSplit(Array(0.8, 0.2), seed = 123)

    // Print the shape of the training and testing sets
    println("Training set shape:")
    println(s"X_train: ${trainDF.count()} rows, ${trainDF.columns.length} columns")
    println("Testing set shape:")
    println(s"X_test: ${testDF.count()} rows, ${testDF.columns.length} columns")



    // Check if the 'bag_of_words' column exists in the DataFrames
    if (bagOfWordsDF.columns.contains("bag_of_words")) {
      // Split the DataFrame into training and testing sets
      val Array(trainDF, testDF) = bagOfWordsDF.randomSplit(Array(0.8, 0.2), seed = 123)

      // Extract the feature vectors from the training and testing sets
      val X_train = trainDF.select("bag_of_words").rdd.flatMap {
        case Row(v: Vector) => v.toArray
      }

      val X_test = testDF.select("bag_of_words").rdd.flatMap {
        case Row(v: Vector) => v.toArray
      }

      // Check if RDDs are empty
      if (X_train.isEmpty() || X_test.isEmpty()) {
        println("No feature vectors found in either training or testing set.")
      } else {
        // Find the maximum value in X_train
        val maxInX_train = X_train.max()

        // Find the maximum value in X_test
        val maxInX_test = X_test.max()

        // Print the maximum values
        println(s"Maximum value in X_train: $maxInX_train")
        println(s"Maximum value in X_test: $maxInX_test")
      }
    } else {
      println("The 'bag_of_words' column does not exist in the DataFrame.")
    }


    // Define a MinMaxScaler
    val scaler = new MinMaxScaler()
      .setInputCol("bag_of_words")
      .setOutputCol("scaled_features")

    // Fit the scaler to training data and then transform both training and testing data
    val scalerModel = scaler.fit(trainDF)
    val scaledTrainDF = scalerModel.transform(trainDF)
    val scaledTestDF = scalerModel.transform(testDF)

    // Extract scaled feature vectors from the DataFrames
    val scaledX_train = scaledTrainDF.select("scaled_features").rdd.flatMap {
      case Row(v: Vector) => v.toArray
    }
    val scaledX_test = scaledTestDF.select("scaled_features").rdd.flatMap {
      case Row(v: Vector) => v.toArray
    }

    // Print scaled X_train
    println("Scaled X_train:")
    scaledX_train.foreach(println)

    // Print scaled X_test
    println("Scaled X_test:")
    scaledX_test.foreach(println)



    // Stop SparkSession
    spark.stop()
  }
}
