package OCLCMINING;

import org.apache.spark.api.java.function.Function;

/**
 * Created by lalithan on 11/3/17.
 */
public class DataCleaning implements Function<OCLCBeans, Boolean> {


    public Boolean call(OCLCBeans v1) throws Exception {

        v1.setId(nullCheck(v1.getId(), "id"));
        v1.setCreated_date(nullCheck(v1.getCreated_date(), "created_date"));
        v1.setUpdated_date(nullCheck(v1.getUpdated_date(), "updated_date"));
        //No change for provider uid and provider name
        //v1.setProvider_uid(nullCheck(v1.getProvider_uid()));
        //v1.setProvider_name(nullCheck(v1.getProvider_name()));
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
        // this needs to be modified to read  v1.setCoverage_start(trimData(v1.getCoverage_start()));
        // this needs to be modified  v1.setCoverage_end(trimData(v1.getCoverage_end()));
        v1.setCoverage_start(nullCheck(v1.getCoverage_start(), "coverage_start"));
        v1.setCoverage_end(nullCheck(v1.getCoverage_end(), "coverage_end"));
        v1.setOpenaccess(nullCheck(v1.getOpenaccess(), "openaccess")); // no clean up needed for this
        v1.setOpen(nullCheck(v1.getOpen(), "open")); // not needed
        // this needs to be modified  v1.setHoldings_regid(countSpace(v1.getHoldings_regid())); // how many libraries are to be selected
        v1.setHoldings_regid(nullCheck(v1.getHoldings_regid(), "holdings_regid"));
        v1.setHoldings_instid(nullCheck(v1.getHoldings_instid(), "holdings_instid"));  // not needed
        v1.setIsbns(nullCheck(v1.getIsbns(), "isbns")); // not needed
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


    //Extract the 1st 4 characters
    public static String trimData(String value) {
        return value.length() > 4 ? value.substring(0, 4) : value;

    }


    //Counts space in a string
    public static String countSpace(String value) {

        return String.valueOf(value.split(" ").length);

    }
}
