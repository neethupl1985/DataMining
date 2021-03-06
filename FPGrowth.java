package OCLCMINING;

import org.apache.spark.api.java.function.Function;

/**
 * Created by lalithan on 11/3/17.
 */
public class FPGrowth implements Function<OCLCBeans, Boolean> {


    public Boolean call(OCLCBeans v1) throws Exception {

        v1.setId(nullCheck(v1.getId()));
        v1.setCreated_date(nullCheck(v1.getCreated_date()));
        v1.setUpdated_date(nullCheck(v1.getUpdated_date()));
        v1.setOwner_institution(nullCheck(v1.getOwner_institution()));
        v1.setSource_institution(nullCheck(v1.getSource_institution()));
        v1.setCollection_uid(nullCheck(v1.getCollection_uid()));
        v1.setCollection_name(nullCheck(v1.getCollection_name()));
        //No change for provider uid and provider name
        //v1.setProvider_uid(nullCheck(v1.getProvider_uid()));
        //v1.setProvider_name(nullCheck(v1.getProvider_name()));
        v1.setOclcnum(nullCheck(v1.getOclcnum()));
        v1.setOclcnums(nullCheck(v1.getOclcnums()));
        v1.setIssn(nullCheck(v1.getIssn()));
        v1.setEissn(nullCheck(v1.getEissn()));
        v1.setIsbn(nullCheck(v1.getIsbn()));
        v1.setWorkid(nullCheck(v1.getWorkid()));
        v1.setTitle(nullCheck(v1.getTitle()));
        v1.setScrubtitle(nullCheck(v1.getScrubtitle()));
        v1.setPublisher(nullCheck(v1.getPublisher()));
        v1.setUrl(nullCheck(v1.getUrl()));
        v1.setAuthor(nullCheck(v1.getAuthor()));
        // v1.setContent(nullCheck(v1.getContent()));
        v1.setJkey(nullCheck(v1.getJkey()));
        v1.setBkey(nullCheck(v1.getBkey()));
        v1.setJsid(nullCheck(v1.getJsid()));
        v1.setPubtype(nullCheck(v1.getPubtype()));
        v1.setCoverage_start(nullCheck(v1.getCoverage_start()));
        v1.setCoverage_end(nullCheck(v1.getCoverage_end()));
        v1.setOpenaccess(nullCheck(v1.getOpenaccess()));
        v1.setOpen(nullCheck(v1.getOpen()));
        v1.setHoldings_regid(nullCheck(v1.getHoldings_regid()));
        v1.setHoldings_instid(nullCheck(v1.getHoldings_instid()));
        v1.setIsbns(nullCheck(v1.getIsbns()));
        v1.setUser_oclcnum(nullCheck(v1.getUser_oclcnum()));
        v1.setUser_oclcnums(nullCheck(v1.getUser_oclcnums()));

        return v1 != null;

    }


    public static String nullCheck(String value) {
        if (value == null || value.isEmpty() || value == "NULL") {
            return "Absent";

        } else {
            return "Present";
        }

    }
}
