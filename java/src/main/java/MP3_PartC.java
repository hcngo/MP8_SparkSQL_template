import scala.Tuple2;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.SQLContext;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.Metadata;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.sql.types.StringType;
import org.apache.spark.sql.types.IntegerType;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.net.URL;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.util.Arrays;
import java.util.List;
import java.util.ArrayList;
import java.util.regex.Pattern;
import java.util.zip.GZIPInputStream;
//import java.util.function.Function;

public final class MP3_PartC {
  private static final String FILE_URL = "http://storage.googleapis.com/books/ngrams/books/googlebooks-eng-all-1gram-20120701-a.gz";
  private static final String FILE_GZIP = "gbooks.gz";
  private static final String FILE = "gbooks";


  //  https://www.journaldev.com/966/java-gzip-example-compress-decompress-file
  private static void decompressGzipFile(String gzipFile, String newFile) {
    try {
      FileInputStream fis = new FileInputStream(gzipFile);
      GZIPInputStream gis = new GZIPInputStream(fis);
      FileOutputStream fos = new FileOutputStream(newFile);
      byte[] buffer = new byte[1024];
      int len;
      while ((len = gis.read(buffer)) != -1) {
        fos.write(buffer, 0, len);
      }
      //close resources
      fos.close();
      gis.close();
    } catch (IOException e) {
      e.printStackTrace();
    }

  }

  public static void main(String[] args) throws Exception {
    SparkSession spark = SparkSession
            .builder()
            .appName("MP3")
            .getOrCreate();
    JavaSparkContext sc = new JavaSparkContext(spark.sparkContext());
    SQLContext sqlContext = new SQLContext(sc);
    /*
     * 1. Setup (10 points): Download the gbook file and write a function
     * to load it in an RDD & DataFrame
     */

    if (!new File(FILE).exists()) {
      URL url = new URL(FILE_URL);
      ReadableByteChannel readableByteChannel = Channels.newChannel(url.openStream());
      FileOutputStream fileOutputStream = new FileOutputStream(FILE_GZIP);
      FileChannel fileChannel = fileOutputStream.getChannel();
      fileChannel
              .transferFrom(readableByteChannel, 0, Long.MAX_VALUE);

      decompressGzipFile(FILE_GZIP, FILE);
    }

    // RDD API
    // Columns: 0: place (string), 1: count1 (int), 2: count2 (int), 3: count3 (int)
    JavaRDD<Row> textFileRdd = sc.textFile(FILE).map(l -> {
      String[] parts = l.split("\\t");
      return RowFactory.create(parts[0], Integer.parseInt(parts[1]), Integer.parseInt(parts[2]), Integer.parseInt(parts[3]));
    });

//        textFileRdd.take(10).forEach(System.out::println);

    //Define the schema of the data
    StructType schema = new StructType(new StructField[]
            {
                    new StructField("word", DataTypes.StringType, true, Metadata.empty()),
                    new StructField("count1", DataTypes.IntegerType, true, Metadata.empty()),
                    new StructField("count2", DataTypes.IntegerType, true, Metadata.empty()),
                    new StructField("count3", DataTypes.IntegerType, true, Metadata.empty())
            }
    );

    // Spark SQL - DataSet API
    Dataset<Row> dataset = spark.createDataFrame(textFileRdd, schema);

    /*
     * 3. Filtering (10 points) Count the number of appearances of word 'ATTRIBUTE'
     */
    // Dataset/Spark SQL API
    Dataset<Row> secondDs = dataset.filter((FilterFunction<Row>) row -> row.getString(0).equals("ATTRIBUTE")).selectExpr("count(1)");
    secondDs.show();

    spark.stop();
    sc.stop();
  }
}
