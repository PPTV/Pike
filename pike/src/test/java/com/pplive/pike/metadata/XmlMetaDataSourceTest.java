package com.pplive.pike.metadata;

import org.junit.Ignore;

/**
 * Created by jiatingjin on 2016/7/28.
 */
public class XmlMetaDataSourceTest {
    String xml="<?xml version=\"1.0\" encoding=\"UTF-8\" standalone=\"yes\"?>\n" +
            "<tables>\n" +
            "    <table>\n" +
            "        <name>tableA</name>\n" +
            "        <columns>\n" +
            "            <column>\n" +
            "\t\t\t\t<name>colA1</name>\n" +
            "\t\t\t\t<type>Boolean</type>\n" +
            "\t\t\t</column>\n" +
            "\t\t\t<column>\n" +
            "\t\t\t\t<name>colA2</name>\n" +
            "\t\t\t\t<type>Double</type>\n" +
            "\t\t\t</column>\n" +
            "\t\t\t<column>\n" +
            "\t\t\t\t<name>colA3</name>\n" +
            "\t\t\t\t<type>String</type>\n" +
            "\t\t\t</column>\n" +
            "        </columns>\n" +
            "    </table>\n" +
            "\t<table>\n" +
            "        <name>tableB</name>\n" +
            "        <columns>\n" +
            "            <column>\n" +
            "\t\t\t\t<name>colB1</name>\n" +
            "\t\t\t\t<type>Int</type>\n" +
            "\t\t\t</column>\n" +
            "        </columns>\n" +
            "    </table>\n" +
            "</tables>";

    @Ignore
    public void test() {
        XmlMetaDataProvider dataSource = XmlMetaDataProvider.createDirectly(xml);
    }
}
