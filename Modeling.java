package OCLCMINING;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.sql.*;

/**
 * Created by lalithan on 11/2/17.
 */
public class Modeling {


    public static void main(String[] args) throws Exception {


        SparkConf sparkConf = new SparkConf();
        sparkConf.setMaster("local");
        sparkConf.setAppName("DataMiningCleanUp");
        SparkContext sparky = new SparkContext(sparkConf);
        SparkSession spark = SparkSession
                .builder()
                .appName("DataMining")
//                .config("spark.some.config.option", "some-value")
                .getOrCreate();
        try (final JavaSparkContext context = new JavaSparkContext(sparky)) {

            JavaRDD<String> abc = context.textFile("/Users/lalithan/Downloads/unmatched_records.txt");

            JavaRDD<OCLCBeans> oclcRDD = abc.map(new Function<String, String>() {


                //filter all the record that contain less than 49 columns
                @Override
                public String call(String v1) throws Exception {
                    if (v1.split("\\t").length == 49) {
                        return v1;
                    } else {
                        return null;
                    }
                }
            }).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String v1) throws Exception {
                    return v1 != null;
                }
            }).map(line -> {
                String[] parts = line.split("\\t");
                OCLCBeans oclc = new OCLCBeans();
                oclc.setId(parts[0]);
                oclc.setCreated_date(parts[2]);
                oclc.setUpdated_date(parts[3]);
                oclc.setOwner_institution(parts[4]);
                oclc.setSource_institution(parts[5]);
                oclc.setCollection_uid(parts[6]);
                oclc.setCollection_name(parts[7]);
                oclc.setProvider_uid(parts[8]);
                oclc.setProvider_name(parts[9]);
                oclc.setOclcnum(parts[12]);
                oclc.setOclcnums(parts[13]);
                oclc.setIssn(parts[15]);
                oclc.setEissn(parts[16]);
                oclc.setIsbn(parts[18]);
                oclc.setWorkid(parts[19]);
                oclc.setTitle(parts[20]);
                oclc.setScrubtitle(parts[21]);
                oclc.setPublisher(parts[22]);
                oclc.setUrl(parts[23]);
                oclc.setAuthor(parts[24]);
                oclc.setContent(parts[25]);
                oclc.setJkey(parts[26]);
                oclc.setBkey(parts[27]);
                oclc.setJsid(parts[28]);
                oclc.setPubtype(parts[29]);
                oclc.setCoverage_start(parts[31]);
                oclc.setCoverage_end(parts[32]);
                oclc.setOpenaccess(parts[38]);
                oclc.setOpen(parts[39]);
                oclc.setHoldings_regid(parts[42]);
                oclc.setHoldings_instid(parts[43]);
                oclc.setIsbns(parts[46]);
                oclc.setUser_oclcnum(parts[47]);
                oclc.setUser_oclcnums(parts[48]);
                return oclc;
            });


            JavaRDD<OCLCBeans> cleanRDD = oclcRDD.filter(new DataCleaning());


            JavaRDD<OCLCBeans> fpGrowthRDD = oclcRDD.filter(new FPGrowth());


            // Apply a schema to an RDD of JavaBeans to get a DataFrame
            Dataset<Row> oclcDF = spark.createDataFrame(fpGrowthRDD, OCLCBeans.class);
            oclcDF.createOrReplaceTempView("OCLC");

            //transformation
            Dataset<Row> tableDF = spark.sql("SELECT * from OCLC");
            Encoder<String> stringEncoder = Encoders.STRING();
            tableDF.toJavaRDD().saveAsTextFile("/Users/lalithan/Downloads/DataMing/output");
            // modifiedRDD.saveAsTextFile("/Users/lalithan/Downloads/DataMing/output");


        }
    }


}
