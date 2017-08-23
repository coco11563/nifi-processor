package pub.sha0w.nifi.processors.model;

import pub.sha0w.nifi.processors.DemoDuplicateRemoveProcessor;

public class DuliModel {
    public DuliModel(String in){
        String[] temp = split(in);
        if (temp.length == 2) {
            mainKey = Key.valueOf(temp[0]);
            vagueKey = Key.valueOf(split(in)[1]);
        } else if (temp.length == 1){
            if (in.charAt(0) == ';') {
                mainKey = Key.valueOf("");
                vagueKey = Key.valueOf(temp[0]);
            } else {
                mainKey = Key.valueOf(temp[0]);
                vagueKey = Key.valueOf("");
            }
        } else {
            mainKey = Key.valueOf("");
            vagueKey = Key.valueOf("");
        }
    }
    private Key mainKey;
    private Key vagueKey;
    public String getArgs() {
        StringBuilder sb = new StringBuilder();
        for (String s : mainKey.getMain()) {
            if (s.contains("&")) {
                String[] temp = s.split("&");
                for (String t : temp) {
                    sb.append(t).append(",");
                }
            } else {
                sb.append(s).append(",");
            }
        }
        for (String s : vagueKey.getMain()) {
            sb.append(s).append(",");
        }
        String ret = sb.toString();
        String r = ret.substring(0, ret.length() - 1);
        return r;
    }
    public Key getVagueKey() {
        return vagueKey;
    }

    public Key getMainKey() {
        return mainKey;
    }
    private String[] split(String in) {
        return in.split(";");
    }

    public static DuliModel valueOf(String s) {
        return new DuliModel(s);
    }

    public static DuliModel valueOf(DemoDuplicateRemoveProcessor.MODEL_ENUM model_enum) {
        switch (model_enum) {
            case PERSON: return new DuliModel(PERSON);
            case PJ_PERSON: return new DuliModel(PJ_PERSON);
            case ACHIEVEMENT_DBLP_JNL: return new DuliModel(ACHIEVEMENT_DBLP_JNL);
            case ACHIEVEMENT_DBLP_URL: return new DuliModel(ACHIEVEMENT_DBLP_URL);
            case ACHIEVEMENT_ISIS_JNL: return new DuliModel(ACHIEVEMENT_ISIS_JNL);
            case ACHIEVEMENT_DBLP_BOOK: return new DuliModel(ACHIEVEMENT_DBLP_BOOK);
            case ACHIEVEMENT_DBLP_DATA: return new DuliModel(ACHIEVEMENT_DBLP_DATA);
            case ACHIEVEMENT_ISIS_BOOK: return new DuliModel(ACHIEVEMENT_ISIS_BOOK);
            case ACHIEVEMENT_ISIS_CONF: return new DuliModel(ACHIEVEMENT_ISIS_CONF);
            case ACHIEVEMENT_ISIS_HOST: return new DuliModel(ACHIEVEMENT_ISIS_HOST);
            case ACHIEVEMENT_DBLP_PAPER: return new DuliModel(ACHIEVEMENT_DBLP_PAPER);
            case ACHIEVEMENT_ISIS_BONUS: return new DuliModel(ACHIEVEMENT_ISIS_BONUS);
            case ACHIEVEMENT_ISIS_OTHER: return new DuliModel(ACHIEVEMENT_ISIS_OTHER);
            case ACHIEVEMENT_ISIS_TRANS: return new DuliModel(ACHIEVEMENT_ISIS_TRANS);
            case ACHIEVEMENT_DBLP_THESIS: return new DuliModel(ACHIEVEMENT_DBLP_THESIS);
            case ACHIEVEMENT_ISIS_PATENT: return new DuliModel(ACHIEVEMENT_ISIS_PATENT);
            case ACHIEVEMENT_ISIS_TALENT: return new DuliModel(ACHIEVEMENT_ISIS_TALENT);
            case ACHIEVEMENT_ISIS_SOFTWARE: return new DuliModel(ACHIEVEMENT_ISIS_SOFTWARE);
            case ACHIEVEMENT_ISIS_STANDARD: return new DuliModel(ACHIEVEMENT_ISIS_STANDARD);
            case ACHIEVEMENT_DBLP_PJ_PERSON: return new DuliModel(ACHIEVEMENT_DBLP_PJ_PERSON);
            case ACHIEVEMENT_ISIS_CONF_REPORT: return new DuliModel(ACHIEVEMENT_ISIS_CONF_REPORT);
            default:return null;
        }
    }
    /*
    BASE ON 详细设计 v1.0
    条件内容用“，”分隔，详细条件与模糊条件按照“；”来进行分隔，与和或以“&”和“|”进行标识
     */
    private final static String PERSON = "ID_CARD,OFFICER_NUMBER,DRIVERLICENSE,ORCID,ISNI,OpenID,ZH_NAME&&EMAIL,ZH_NAME&&MOBILE;ZH_NAME,ORG_NAME,TITLE,PROF_TITLE";
    private final static String PJ_PERSON = "PSN_CODE,ZH_NAME&&EMAIL,ZH_NAME&&MOBILE;ZH_NAME,ORG_NAME,PROF_TITLE_NAME,POSITION";
    private final static String ACHIEVEMENT_DBLP_JNL = "Doi,ARTICLE_NO;ZH_NAME|EN_NAME,PUBLISH_YEAR,JOURNAL_NAME,VOLUME,SERIES,PAGE_RANGE";
    private final static String ACHIEVEMENT_DBLP_PAPER = "Doi,ARTICLE_NO;ZH_NAME|EN_NAME,PUBLISH_YEAR,PROCEEDING_NAME,PROCEEDING_ADD,START_TIME,END_TIME";
    private final static String ACHIEVEMENT_DBLP_BOOK = "ISBN;ZH_NAME|EN_NAME,PUBLISH_YEAR,PUBLISHER,PUBLISH_TIME,BOOK_SERIES_NAME,PAGE_RANGE";
    private final static String ACHIEVEMENT_DBLP_THESIS = "PATENT_NO,IPC,CPC;ZH_NAME|EN_NAME,PUBLISH_YEAR,PATENTEE,APPLICANT,ISSUING_UNIT,PATENT_TYPE";
    private final static String ACHIEVEMENT_DBLP_URL = ";ZH_NAME|EN_NAME,PUBLISH_YEAR,CONFERENCE_TYPE,REPORT_TYPE,CONFERENCE_ZH_NAME|CONFERENCE_EN_NAME";
    private final static String ACHIEVEMENT_DBLP_DATA = "CRITERION_NO;ZH_NAME|EN_NAME,PUBLISH_YEAR,CRITERION_TYPE,PUBLISHED_AGENCIES";
    private final static String ACHIEVEMENT_DBLP_PJ_PERSON = "REGISTRATION_NUMBER;";
    private final static String ACHIEVEMENT_ISIS_JNL = "ISSUE_BY&REWARD_NUMBER;ZH_NAME|EN_NAME,PUBLISH_YEAR,ISSUED_BY,REWARD_TYPE,REWARD_RANK";
    private final static String ACHIEVEMENT_ISIS_CONF = "ID_CARD;ZH_NAME|EN_NAME,PUBLISH_YEAR,TRAINING_TYPE,TALENT_TYPE,PROFESSOR_COOPERATION";
    private final static String ACHIEVEMENT_ISIS_BOOK = ";ZH_NAME|EN_NAME,CONFERENCE_TYPE,START_TIME,END_TIME,CONFERENCE";
    private final static String ACHIEVEMENT_ISIS_PATENT = ";ZH_NAME|EN_NAME,PRODUCT_TYPE,TRANSFER_FORM,TURNOVER,FAVOREE";
    private final static String ACHIEVEMENT_ISIS_CONF_REPORT = ";ZH_NAME|EN_NAME,RESEARCH_TYPE,DECLARATION,SHARED_SCOPE";
    private final static String ACHIEVEMENT_ISIS_STANDARD = "Doi,NUMBER;ZH_NAME|EN_NAME,PUBLISH_YEAR,JOURNAL_NAME,VOLUME,SERIES,PAGE_RANGE";
    private final static String ACHIEVEMENT_ISIS_SOFTWARE = "Doi;ZH_NAME|EN_NAME,PUBLISH_YEAR,PROCEEDING_NAME,PROCEEDING_ADD,START_TIME,END_TIME";
    private final static String ACHIEVEMENT_ISIS_BONUS = "ISBN;ZH_NAME|EN_NAME,PUBLISH_YEAR,PUBLISHER,PUBLISH_TIME,BOOK_SERIES_NAME,PAGE_RANGE";
    private final static String ACHIEVEMENT_ISIS_TALENT = ";TITLE,AUTHOR,SCHOOL,YEAR";
    private final static String ACHIEVEMENT_ISIS_HOST = "url;TITLE,AUTHOR,YEAR";
    private final static String ACHIEVEMENT_ISIS_TRANS = ";";
    private final static String ACHIEVEMENT_ISIS_OTHER = ";";


}
