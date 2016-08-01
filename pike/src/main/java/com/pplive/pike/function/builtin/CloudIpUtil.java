package com.pplive.pike.function.builtin;

import java.io.File;
import java.util.HashMap;
import java.util.Map;
import java.util.Timer;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.pplive.pike.Configuration;
import com.pplive.pike.base.AbstractUDF;
import com.pplive.pike.util.CodeManager;
import com.pplive.pike.util.Path;
//import com.pptv.cdn.iplib.api.IPLib;

/**
 * 用云播部门提供的jar与IP地址数据库解析IP，得到国家，区域，城市，ISP的名称与编号。
 * 
 * @author mingyan
 */
public class CloudIpUtil {

    public static final Logger log = LoggerFactory.getLogger(CloudIpUtil.class);

    /** 未知的IP或者非法的IP值时，返回的代码值为0. */
    private static final int UNKNOWN_CODE = 0;

    /** 未知的IP或者非法的IP值时，返回的名称为未知. */
    private static final String UNKNOWN_NAME = "未知";

 //   private static IPLib iplib;

    private static CodeManager codeManager;

    /**
     * <ol>
     * <li>国家，地区，城市与ISP数据保持不变，只在启动时载入一次。
     * <li>IP地址库会每天更新一次。
     * </ol>
     */
 /*   static {
        try {
            Configuration conf = new Configuration();
            String codeLibraryDir =
                Path.combine(System.getenv(Configuration.PIKE_CONF_DIR_KEY),
                    (String) conf.get(Configuration.CloudplayIPDictionaryDir));
            log.info("尝试载入国家，区域，城市与ISP资源");
            log.info("PIKE_CONF_DIR_KEY:" + System.getenv(Configuration.PIKE_CONF_DIR_KEY));
            log.info("CloudplayIPDictionaryDir:" + (String) conf.get(Configuration.CloudplayIPDictionaryDir));
            log.info("codeLibraryDir:" + codeLibraryDir);
            codeManager = new CodeManager(codeLibraryDir);
            codeManager.init();
            log.info("成功载入国家，区域，城市，ISP资源");

            String iplibConfigFile =
                System.getenv(Configuration.PIKE_CONF_DIR_KEY) + File.separator + "iplib-api4j.xml";

            iplib = IPLib.createIPLib(iplibConfigFile);

            int syncIntervalInMinutes = 60 * 24;
            try {
                syncIntervalInMinutes =
                    Integer.parseInt((String) conf.get(Configuration.CloudplayIPSyncIntervalInMinutes));
            } catch (NumberFormatException e) {
                log.warn("Fail to get IP sync interval, use default ", e);
            }
            new Timer(true).schedule(iplib, 60 * 1000, syncIntervalInMinutes * DateUtils.MILLIS_PER_MINUTE);
            log.info("成功IP地址库并且启动IP地址库更新定时任务，间隔为： " + syncIntervalInMinutes + " 分钟。");
        } catch (Exception e) {
            e.printStackTrace();
            throw new IllegalStateException("Error happen when load ip library: " + e);
        }
    } */

    /**
     * 这个类提供IP所对应的名称与代码信息。 IPLib返回IP的地理信息，格式：国家/省份/城市/ISP
     * 
     * @author mingyan
     */
    public static class IPCodeInfoMap extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Map<String, String> evaluate(String ip) {
            Map<String, String> result = new HashMap<String, String>();

            String countryName = UNKNOWN_NAME;
            String areaName = UNKNOWN_NAME;
            String cityName = UNKNOWN_NAME;
            String ISPName = UNKNOWN_NAME;

            int countryCode = UNKNOWN_CODE;
            int areaCode = UNKNOWN_CODE;
            int cityCode = UNKNOWN_CODE;
            int ISPCode = UNKNOWN_CODE;
            if (!StringUtils.isEmpty(ip)) {
                //String info = iplib.queryIP(ip);
                String info = null;
                String[] fields = info.split("/");

                if (fields.length >= 4) {
                    countryName = fields[0];
                    areaName = fields[1];
                    cityName = fields[2];
                    ISPName = fields[3];

                    countryCode = codeManager.getCountryCode(countryName);
                    areaCode = codeManager.getAreaCode(areaName);
                    cityCode = codeManager.getCityCode(cityName);
                    ISPCode = codeManager.getISPCode(ISPName);
                }
            }
            result.put("CountryCode", String.valueOf(countryCode));
            result.put("AreaCode", String.valueOf(areaCode));
            result.put("CityCode", String.valueOf(cityCode));
            result.put("ISPCode", String.valueOf(ISPCode));

            result.put("CountryName", countryName);
            result.put("AreaName", areaName);
            result.put("CityName", cityName);
            result.put("ISPName", ISPName);
            return result;
        }
    }

    /**
     * 这个类提供IP所对应的名称。 IPLib返回IP的地理信息，格式：国家/省份/城市/ISP
     * 
     * @author mingyan
     */
    public static class IPInfoMap extends AbstractUDF {
        private static final long serialVersionUID = 1L;

        public static Map<String, String> evaluate(String ip) {
            Map<String, String> result = new HashMap<String, String>();

            String countryName = UNKNOWN_NAME;
            String areaName = UNKNOWN_NAME;
            String cityName = UNKNOWN_NAME;
            String ISPName = UNKNOWN_NAME;

            if (!StringUtils.isEmpty(ip)) {
                //String info = iplib.queryIP(ip);
                String info = null;
                String[] fields = info.split("/");

                if (fields.length >= 4) {
                    countryName = fields[0];
                    areaName = fields[1];
                    cityName = fields[2];
                    ISPName = fields[3];
                }
            }
            result.put("CountryName", countryName);
            result.put("AreaName", areaName);
            result.put("CityName", cityName);
            result.put("ISPName", ISPName);
            return result;
        }
    }

    /**
     * 返回IP所在段
     * 
     * @author jiatingjin
     *
     */
    public static class IPRange extends AbstractUDF {

        /**
         * 
         */
        private static final long serialVersionUID = 1L;
        
        public static String evaluate(String ip) {
            return null;//iplib.queryIPRangeByIP(ip);
        }
    }
}
