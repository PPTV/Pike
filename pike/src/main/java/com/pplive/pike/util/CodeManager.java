package com.pplive.pike.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.nio.charset.Charset;
import java.util.HashMap;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

/**
 * 根据码表，生成Map，供Pike查询国家，区域，城市与运营商对应的编码。
 * 
 * @author mingyan
 */
public class CodeManager {

    public static final Log log = LogFactory.getLog(CodeManager.class);

    /**
     * 码表文件存放目录
     */
    private final String baseDirectory;

    /**
     * 国家码表Hash
     */
    private HashMap<String, Integer> HS_Country;

    /**
     * 省份码表Hash
     */
    private HashMap<String, Integer> HS_Area;

    /**
     * 城市码表Hash
     */
    private HashMap<String, Integer> HS_City;

    /**
     * 运营商码表Hash
     */
    private HashMap<String, Integer> HS_ISP;

    /** 未知码。 */
    private static final int CODE_UNKNOW = 0;

    /**
     * 构造函数，初始化目录
     * 
     * @param baseDirectory
     *            码表文件存放的根目录
     */
    public CodeManager(String baseDirectory) {
        if (baseDirectory.endsWith(File.separator)) {
            this.baseDirectory = baseDirectory;
        } else {
            this.baseDirectory = baseDirectory + File.separator;
        }
    }

    /**
     * 初始化，载入需要的资源
     */
    public void init() {
        loadCountry();
        loadArea();
        loadCity();
        loadISP();
    }

    /**
     * 获得国家代码
     * 
     * @param countryName
     *            国家名称
     * @return String 国家代码
     */
    public int getCountryCode(String countryName) {
        int countryCode = CODE_UNKNOW;
        if (HS_Country.containsKey(countryName)) {
            countryCode = HS_Country.get(countryName);
        }
        return countryCode;
    }

    /**
     * 返回省份码
     * 
     * @param areaName
     *            省份名称
     * @return String 省份码
     */
    public int getAreaCode(String areaName) {
        int areaCode = CODE_UNKNOW;

        if (HS_Area.containsKey(areaName)) {
            areaCode = HS_Area.get(areaName);
        }
        return areaCode;
    }

    /**
     * 返回城市码
     * 
     * @param cityName
     *            城市名称
     * @return String 城市
     */
    public int getCityCode(String cityName) {
        int cityCode = CODE_UNKNOW;
        if (HS_City.containsKey(cityName)) {
            cityCode = HS_City.get(cityName);
        }
        return cityCode;
    }

    /**
     * 返回运营商码
     * 
     * @param ispName
     *            运营商名称
     * @return String 运营商
     */
    public int getISPCode(String ispName) {
        int ISPCode = CODE_UNKNOW;
        if (HS_ISP.containsKey(ispName)) {
            ISPCode = HS_ISP.get(ispName);
        }
        return ISPCode;
    }

    /**
     * 载入国家码表
     */
    private void loadCountry() {
        HS_Country = new HashMap<String, Integer>();
        String fileName = this.baseDirectory + "countryCode.txt";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), Charset.forName("utf8")));
            String query = null;
            while ((query = br.readLine()) != null) {
                String[] temp = query.split("\t");
                HS_Country.put(temp[1], Integer.parseInt(temp[0]));
            }
        } catch (Exception e) {
            String msg = "Fail to load Country code from " + fileName + ", " + e.getMessage();
            log.error(msg, e);
            throw new IllegalStateException(msg);
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (Exception e) {
                log.warn("Fail to close file ", e);
            }
        }
    }

    /**
     * 载入省份码表
     */
    private void loadArea() {
        HS_Area = new HashMap<String, Integer>();
        String fileName = this.baseDirectory + "areaCode.txt";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), Charset.forName("utf8")));
            String query = null;
            while ((query = br.readLine()) != null) {
                String[] temp = query.split("\t");
                HS_Area.put(temp[2], Integer.parseInt(temp[1]));
            }
        } catch (Exception e) {
            String msg = "Fail to load Area code from " + fileName + ", " + e.getMessage();
            log.error(msg, e);
            throw new IllegalStateException(msg);
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (Exception e) {
                log.warn("Fail to close file ", e);
            }
        }
    }

    /**
     * 载入城市码表
     */
    private void loadCity() {
        HS_City = new HashMap<String, Integer>();
        String fileName = this.baseDirectory + "cityCode.txt";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), Charset.forName("utf8")));
            String query = null;
            while ((query = br.readLine()) != null) {
                String[] temp = query.split("\t");
                HS_City.put(temp[3], Integer.parseInt(temp[2]));
            }
        } catch (Exception e) {
            String msg = "Fail to load City code from " + fileName + ", " + e.getMessage();
            log.error(msg, e);
            throw new IllegalStateException(msg);
        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (Exception e) {
                log.warn("Fail to close file ", e);
            }
        }
    }

    /**
     * 载入ISP码表
     */
    private void loadISP() {
        HS_ISP = new HashMap<String, Integer>();
        String fileName = this.baseDirectory + "ispCode.txt";
        BufferedReader br = null;
        try {
            br = new BufferedReader(new InputStreamReader(new FileInputStream(fileName), Charset.forName("utf8")));
            String query = null;
            while ((query = br.readLine()) != null) {
                String[] temp = query.split("\t");
                HS_ISP.put(temp[1], Integer.parseInt(temp[0]));
            }
        } catch (Exception e) {
            String msg = "Fail to load ISP code from " + fileName + ", " + e.getMessage();
            log.error(msg, e);
            throw new IllegalStateException(msg);

        } finally {
            try {
                if (br != null)
                    br.close();
            } catch (Exception e) {
                log.warn("Fail to close file ", e);
            }
        }
    }
}
