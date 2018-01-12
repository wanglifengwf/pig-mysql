package com.tsl.pig.udf;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import java.io.IOException;

public class StringUdf extends EvalFunc<String> {

    private static final String del = System.getProperty("line.separator");

    @Override
    public String exec(Tuple input) throws IOException {
        if (input == null || input.size() == 0)
            return null;
        try{
            String str = (String)input.get(0);
            return str.replaceAll(del,"");
        }catch(Exception e){
            throw new RuntimeException(e);
        }
    }

}
