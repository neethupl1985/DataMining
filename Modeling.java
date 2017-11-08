package OCLCMINING;

import org.apache.spark.SparkConf;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.fpm.FPGrowth;
import org.apache.spark.mllib.fpm.FPGrowthModel;
import org.apache.spark.sql.*;
import org.spark_project.guava.collect.Lists;

import java.util.ArrayList;

/**
 * Created by lalithan on 11/2/17.
 */
public class Modeling {


    //remove jsio

    public static void main(String[] args) throws Exception {


        //Creating spark configuration
        SparkConf sparkConf = new SparkConf();
        //setting up spark to run locally and not in cluster
        sparkConf.setMaster("local");
        //sAssigning application name
        sparkConf.setAppName("DataMiningCleanUp");
        SparkContext sparky = new SparkContext(sparkConf);
        SparkSession spark = SparkSession
                .builder()
                .appName("DataMining")
                .getOrCreate();
        try (final JavaSparkContext context = new JavaSparkContext(sparky)) {

            //Reading data from unmatched_records.txt
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
                oclc.setProvider_uid(parts[8]);
                oclc.setProvider_name(parts[9]);
                oclc.setOclcnum(parts[12]);
                oclc.setOclcnums(parts[13]);
                oclc.setIssn(parts[15]);
                oclc.setEissn(parts[16]);
                oclc.setIsbn(parts[18]);
                oclc.setTitle(parts[20]);
                oclc.setPublisher(parts[22]);
                oclc.setUrl(parts[23]);
                oclc.setAuthor(parts[24]);
                oclc.setContent(parts[25]);
                oclc.setJkey(parts[26]);
                oclc.setBkey(parts[27]);
                oclc.setPubtype(parts[29]);
                oclc.setCoverage_start(parts[31]);
                oclc.setCoverage_end(parts[32]);
                oclc.setOpenaccess(parts[38]);
                oclc.setHoldings_regid(parts[42]);
                oclc.setHoldings_instid(parts[43]);
                oclc.setIsbns(parts[46]);
                oclc.setUser_oclcnum(parts[47]);
                oclc.setUser_oclcnums(parts[48]);
                return oclc;
            });


            //Clean RDD contains the cleaned up data
            JavaRDD<OCLCBeans> cleanRDD = oclcRDD.filter(new DataCleaning());


            // Apply a schema to an RDD of JavaBeans to get a DataFrame
            Dataset<Row> oclcDF = spark.createDataFrame(cleanRDD, OCLCBeans.class);
            oclcDF.createOrReplaceTempView("OCLC");


            Dataset<Row> tableDF = spark.sql("SELECT id,created_date,updated_date" +
                    ",provider_uid,oclcnum,oclcnums,issn,eissn,isbn," +
                    "title,publisher,url,author,content,jkey,bkey,pubtype,coverage_start" +
                    ",coverage_end,openaccess,holdings_regid,holdings_instid,isbns," +
                    "user_oclcnum,user_oclcnums from OCLC ");
            tableDF.show();
            tableDF.toJavaRDD().saveAsTextFile("/Users/lalithan/Downloads/DataMing/output");


            JavaRDD<ArrayList<String>> transactions = context.textFile("/Users/lalithan/Downloads/DataMing/output").map(
                    new Function<String, ArrayList<String>>() {
                        @Override
                        public ArrayList<String> call(String s) {

                            return Lists.newArrayList(s.split(","));
                        }
                    }
            );


            FPGrowthModel<String> model = new FPGrowth()
                    .setMinSupport(0.9)
                    .setNumPartitions(10)
                    .run(transactions);


            model.freqItemsets().saveAsTextFile("/Users/lalithan/Downloads/DataMing/text");


            //filter output for absent_oclcnum
            //check for specifying confidence(Dr stantons guidelines)
            //lift factor how to specify
            // decending order on frequency
            //dont give user_oclcnum and nums
            // find the highest number
           /* JavaRDD<String> modelOutput = context.textFile("/Users/lalithan/Downloads/DataMing/text").map(
                    new Function<String, String>() {

                        String splitLine[];

                        @Override
                        public String call(String v1) throws Exception {
                            String count;
                            try {
                                count = v1.split(":")[2];
                                if (Integer.parseInt(count) > 100000) {
                                    return v1;
                                } else {
                                    return null;
                                }

                            } catch (Exception e) {
                                return null;
                            }

                        }
                    }
            ).filter(new Function<String, Boolean>() {
                @Override
                public Boolean call(String v1) throws Exception {
                    return v1 != null;
                }
            });

            modelOutput.saveAsTextFile("/Users/lalithan/Downloads/DataMining/finalCounts");
*/
        }
    }


}
