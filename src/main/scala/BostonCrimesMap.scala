import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.expressions.Window

case class BostonCrimesMap(
                            DISTRICT: String,
                            crimes_total: Int,
                            crimes_monthly: Int,
                            frequent_crime_types: String,
                            lat: Float,
                            lng: Float
                          )

object BostonCrimesMap {
  def main(args:Array[String]) :Unit = {
    val spark = SparkSession.builder()
      .appName("boston_aksenenko")
      .master("local[*]")
      .getOrCreate()
    import spark.implicits._

    val schema_crime = new StructType()
      .add($"INCIDENT_NUMBER".string)
      .add($"OFFENSE_CODE".int)
      .add($"OFFENSE_CODE_GROUP".string)
      .add($"OFFENSE_DESCRIPTION".string)
      .add($"DISTRICT".string)
      .add($"REPORTING_AREA".string)
      .add($"SHOOTING".string)
      .add($"OCCURRED_ON_DATE".string)
      .add($"YEAR".string)
      .add($"MONTH".string)
      .add($"DAY_OF_WEEK".string)
      .add($"HOUR".string)
      .add($"UCR_PART".string)
      .add($"STREET".string)
      .add($"Lat".string)
      .add($"Long".string)
      .add($"Location".string)

    val df_crime = spark.read.format("csv").option("header", "true").schema(schema_crime)
      .load(args(0))
//      .load("C:\\Users\\Aleksei\\Desktop\\DataEngineerOtus\\7.crime\\crime.csv")
    val df_crime_dist = df_crime.dropDuplicates()

    val schema_offense_codes = new StructType()
      .add($"CODE".int)
      .add($"NAME".string)

    val df_offense_codes = spark.read.format("csv").option("header", "true").schema(schema_offense_codes)
      .load(args(1))
//      .load("C:\\Users\\Aleksei\\Desktop\\DataEngineerOtus\\7.crime\\offense_codes.csv")
    val df_offense_codes_1 = df_offense_codes.dropDuplicates("CODE")
    val df_offense_codes_dist = broadcast(df_offense_codes_1)

    // Задание № 1
    val df_crime_usl_1 = df_crime_dist
      .filter(($"DISTRICT".isNotNull) && ($"DISTRICT" =!= ""))
      .groupBy($"DISTRICT").agg(count($"DISTRICT").as("crimes_total"))
      .select($"DISTRICT", $"crimes_total")

    // Задание № 2
    val df_crime_usl_2 = df_crime_dist
      .filter(($"DISTRICT".isNotNull) && ($"DISTRICT" =!= ""))
      .groupBy($"DISTRICT", $"YEAR", $"MONTH")
      .agg(count($"DISTRICT").as("COUNT"))
      .select($"DISTRICT", $"COUNT")

    df_crime_usl_2.createOrReplaceTempView("df_crime_usl_2")

    val df_crime_usl_2_x = spark.sql("SELECT DISTRICT, percentile_approx(COUNT, 0.5) AS crimes_monthly " +
      "FROM df_crime_usl_2 GROUP BY DISTRICT ORDER BY DISTRICT")
    //
    //    Задание № 3
    val df_crime_usl_3 = df_crime_dist.as("t1")
      .join(df_offense_codes_dist.as("t2"), $"t1.OFFENSE_CODE"===$"t2.CODE", "left")
      .filter($"t2.NAME".isNotNull && $"t1.DISTRICT".isNotNull)
      .withColumn("frequent_crime_types", regexp_replace($"t2.NAME", "\\s+-.*", ""))
      .groupBy($"t1.DISTRICT", $"frequent_crime_types").count()
      .orderBy(desc("t1.DISTRICT"), desc("count"))
      .withColumn("rank", rank().over(Window.partitionBy($"t1.DISTRICT").orderBy($"count".desc)))
      .filter($"rank" <= 3)
      .groupBy("t1.DISTRICT")
      .pivot("rank")
      .agg(first($"frequent_crime_types"))
      .withColumn("frequent_crime_types", concat_ws(", ", $"1", $"2", $"3"))
      .select($"t1.DISTRICT", $"frequent_crime_types")
    //
    //
    //    Задание № 4 И 5
    val df_crime_usl_4_5 = df_crime_dist
      .filter($"DISTRICT".isNotNull && $"Lat".isNotNull && $"Long".isNotNull)
      .groupBy($"DISTRICT")
      .agg(mean($"Lat").as("lat"), mean($"Long").as("lng"))
    //
    val df_itog = df_crime_usl_1.as("t1")
      .join(df_crime_usl_2_x.as("t2"), $"t1.DISTRICT" === $"t2.DISTRICT")
      .join(df_crime_usl_3.as("t3"), $"t1.DISTRICT" === $"t3.DISTRICT")
      .join(df_crime_usl_4_5.as("t4"), $"t1.DISTRICT" === $"t4.DISTRICT")
      .select($"t1.DISTRICT", $"crimes_total", $"crimes_monthly", $"frequent_crime_types", $"lat", $"lng")
      .orderBy($"t1.DISTRICT")
      .write.parquet(args(2) + "\\temp.parquet")
//      .show()

//        df_itog.repartition(1).write.format("parquet").mode("append")
//          .save(args(2))
//          .save("C:\\Users\\Aleksei\\Desktop\\DataEngineerOtus\\7.crime\\temp.parquet")

  }

}