name := "CFTest"

version := "1.0"

scalaVersion := "2.10.4"

libraryDependencies += "org.apache.spark" %% "spark-core" % "1.3.0"

libraryDependencies += "org.apache.spark" %% "spark-sql" % "1.3.0"

libraryDependencies += "org.apache.spark" %% "spark-graphx" % "1.3.0"

libraryDependencies += "org.apache.spark" %% "spark-hive" % "1.3.0"

libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.3.0"

libraryDependencies += "org.apache.mahout" %% "mahout-core" % "0.9"

resolvers += "mvnrepo" at "http://d181.mzhen.cn/nexus/content/repositories/mvnrepo/"

resolvers += "d181repo" at "http://d181.mzhen.cn/nexus/content/groups/public/"

