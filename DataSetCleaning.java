package OCLCMINING;

import org.apache.spark.api.java.function.Function;

/**
 * Created by lalithan on 11/3/17.
 */
public class DataSetCleaning implements Function<OCLCBeans, Boolean> {


    public Boolean call(OCLCBeans v1) throws Exception {

        v1.setId(v1.getId());
        v1.setCreated_date(v1.getCreated_date());
        v1.setUpdated_date(v1.getUpdated_date());
        v1.setOclcnum(v1.getOclcnum());
        v1.setOclcnums(v1.getOclcnums());
        v1.setIsbn(v1.getIsbn());
        v1.setTitle(replaceCtrl(v1.getTitle())); 
        v1.setPublisher(replaceCtrl(v1.getPublisher()));  
        v1.setUrl(v1.getUrl());
        v1.setAuthor(replaceCtrl(v1.getAuthor()));
        v1.setContent(getFirstValue(v1.getContent()));
        v1.setJkey(v1.getJkey());
        v1.setBkey(v1.getBkey());
        v1.setPubtype(v1.getPubtype());
        v1.setCoverage_start(trimData(v1.getCoverage_start()));
        v1.setCoverage_end(trimData(v1.getCoverage_end()));
        v1.setOpenaccess(v1.getOpenaccess());
        v1.setHoldings_regid(countSpace(v1.getHoldings_regid()));
        v1.setIsbns(countSpace(v1.getIsbns()));
        v1.setUser_oclcnum(v1.getUser_oclcnum());
        v1.setUser_oclcnums(v1.getUser_oclcnums());

        return v1 != null;

    }


    public static String replaceCtrl(String value) {
        if (value != null) {
            value.replaceAll("\\p{Cntrl}", "");
        }
        return value;
    }


    //Extract the 1st 4 characters
    public static String trimData(String value) {
        return value.length() > 4 ? value.substring(0, 4) : value;

    }


    //Counts space in a string
    public static String countSpace(String value) {

        return String.valueOf(value.split(" ").length);

    }


    //Counts space in a string
    public static String getFirstValue(String value) {

        if (value.contains(" ")) {
            return value.split(" ")[0];
        } else
            return value;

    }
}
