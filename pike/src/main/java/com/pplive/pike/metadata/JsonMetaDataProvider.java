package com.pplive.pike.metadata;

import com.google.gson.Gson;
import com.pplive.pike.Configuration;
import com.pplive.pike.client.Table;
import java.io.*;


/**
 * Created by jiatingjin on 2016/7/28.
 */
public class JsonMetaDataProvider implements MetaDataProvider {

    public static final String metafile = "meta.json";

    private Table[] tables;

    @Override
    public void init(Configuration conf){
        ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
        InputStream in = classLoader.getResourceAsStream(metafile);
        Reader reader = new InputStreamReader(in);
        BufferedReader bufferedReader = new BufferedReader(reader);
        Gson gson = new Gson();
        tables = gson.fromJson(reader, Table[].class);

        try {
            if (bufferedReader != null) {
                bufferedReader.close();
            }

            if (reader != null) {
                reader.close();
            }

            if (in != null) {
                in.close();
            }

        } catch (IOException e) {
            e.printStackTrace();
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
