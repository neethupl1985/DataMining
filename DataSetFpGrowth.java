package OCLCMINING;

import org.apache.spark.api.java.function.Function;

/**
 * Created by lalithan on 11/3/17.
 */
public class DataSetFpGrowth implements Function<OCLCBeans, Boolean> {


    public Boolean call(OCLCBeans v1) throws Exception {

        v1.setId(nullCheck(v1.getId(), "id"));
        v1.setCreated_date(nullCheck(v1.getCreated_date(), "created_date"));
        v1.setUpdated_date(nullCheck(v1.getUpdated_date(), "updated_date"));
        v1.setOclcnum(nullCheck(v1.getOclcnum(), "oclcnum"));
        v1.setOclcnums(nullCheck(v1.getOclcnums(), "oclcnums"));
        v1.setIssn(nullCheck(v1.getIssn(), "issn"));
        v1.setEissn(nullCheck(v1.getEissn(), "eissn"));
        v1.setIsbn(nullCheck(v1.getIsbn(), "isbn"));
        v1.setTitle(nullCheck(v1.getTitle(), "title"));
        v1.setPublisher(nullCheck(v1.getPublisher(), "publisher"));
        v1.setUrl(nullCheck(v1.getUrl(), "url"));
        v1.setAuthor(nullCheck(v1.getAuthor(), "author"));
        v1.setContent(nullCheck(v1.getContent(), "content"));
        v1.setJkey(nullCheck(v1.getJkey(), "jkey"));
        v1.setBkey(nullCheck(v1.getBkey(), "bkey"));
        v1.setPubtype(nullCheck(v1.getPubtype(), "pubtype"));
        v1.setCoverage_start(nullCheck(v1.getCoverage_start(), "coverage_start"));
        v1.setCoverage_end(nullCheck(v1.getCoverage_end(), "coverage_end"));
        v1.setOpenaccess(nullCheck(v1.getOpenaccess(), "openaccess"));
        v1.setHoldings_regid(nullCheck(v1.getHoldings_regid(), "holdings_regid"));
        v1.setUser_oclcnum(nullCheck(v1.getUser_oclcnum(), "user_oclcnum"));
        v1.setUser_oclcnums(nullCheck(v1.getUser_oclcnums(), "user_oclcnums"));

        return v1 != null;

    }


    public static String nullCheck(String value, String appendString) {
        if (value == null || value.isEmpty() || value == "NULL") {

            return "Absent" + "_" + appendString;

        } else {
            value.replaceAll("\\p{Cntrl}", "");
            if (value.isEmpty()) {
                return "Absent" + "_" + appendString;
            }


            return "Present" + "_" + appendString;
        }
    }


}
