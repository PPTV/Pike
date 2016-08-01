package com.pplive.pike.metadata;

import com.pplive.pike.Configuration;
import com.pplive.pike.util.SerializeUtils;
import com.pplive.pike.client.Table;

import javax.xml.bind.annotation.XmlElement;
import javax.xml.bind.annotation.XmlRootElement;
import java.io.StringReader;


/**
 * Created by jiatingjin on 2016/7/28.
 */

public class XmlMetaDataProvider implements MetaDataProvider {

    public static final String metafile = "meta.xml";

    private Table[] tables;

    @XmlRootElement(name="tables")
    private static class _Tables {
        @XmlElement(name = "table")
        Table[] _tables;
    }

    public XmlMetaDataProvider() {

    }

    @Override
    public void init(Configuration conf) {
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        String tableInfoFile = classLoader.getResource(metafile).getPath();
        try {
            _Tables _tables = SerializeUtils.xmlDeserialize(_Tables.class, tableInfoFile);
            this.tables = _tables._tables;
        }
        catch(RuntimeException e){
            System.err.println(String.format("read table info failed from file %s", tableInfoFile));
            throw(e);
        }
    }


    public static XmlMetaDataProvider createDirectly(String fileContentXml) {
        try {
            XmlMetaDataProvider result = new XmlMetaDataProvider();
            _Tables _tables = SerializeUtils.xmlDeserialize(_Tables.class, new StringReader(fileContentXml));
            result.tables = _tables._tables;
            return result;
        } catch (RuntimeException e) {
            throw (e);
        }
    }


    @Override
    public Table getTable(String name) {
        for(Table t : this.tables) {
            if (t.getName().equals(name))
                return t;
        }
        return  null;
    }

    @Override
    public String[] getTableNames() {
        String[] names = new String[this.tables.length];
        for(int n = 0; n < names.length; n +=1){
            names[n] = this.tables[n].getName();
        }
        return names;
    }

}
