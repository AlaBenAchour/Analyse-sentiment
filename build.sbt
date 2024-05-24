ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.18"

lazy val root = (project in file("."))
  .settings(
    name := "Analatic-sentiment-SBT",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-core
    libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.0",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-sql
    libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0" % "provided",
    // https://mvnrepository.com/artifact/org.scala-lang/scala-library
    libraryDependencies += "org.scala-lang" % "scala-library" % "2.12.18",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-hive
    libraryDependencies += "org.apache.spark" %% "spark-hive" % "3.5.0" % "provided",
      // https://mvnrepository.com/artifact/org.slf4j/slf4j-api
      libraryDependencies += "org.slf4j" % "slf4j-api" % "2.0.10",
    // https://mvnrepository.com/artifact/org.slf4j/slf4j-simple
    libraryDependencies += "org.slf4j" % "slf4j-simple" % "2.0.10" % Test,
    // https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
    libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.5.0" % "provided",
    // https://mvnrepository.com/artifact/org.apache.spark/spark-mllib
    libraryDependencies += "org.apache.spark" %% "spark-mllib" % "3.5.0" % "provided",
    // https://mvnrepository.com/artifact/org.plotly-scala/plotly-core
    libraryDependencies += "org.plotly-scala" %% "plotly-core" % "0.5.4",
      // https://mvnrepository.com/artifact/org.plotly-scala/plotly-render
      libraryDependencies += "org.plotly-scala" %% "plotly-render" % "0.5.4",
    // https://mvnrepository.com/artifact/com.johnsnowlabs.nlp/spark-nlp
    libraryDependencies += "com.johnsnowlabs.nlp" %% "spark-nlp" % "5.3.0" % Test,
    libraryDependencies += "org.scalafx" %% "scalafx" % "16.0.0-R24",
    libraryDependencies += "org.openjfx" % "javafx-controls" % "16", // or any desired version
    libraryDependencies += "org.openjfx" % "javafx-fxml" % "16", // or any desired version
    libraryDependencies += "org.scalaj" %% "scalaj-http" % "2.4.2",
    libraryDependencies += "com.typesafe.play" %% "play-json" % "2.9.2",
    libraryDependencies += "com.github.tototoshi" %% "scala-csv" % "1.3.6",






  )
