package edu.realtime.util;

import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

/**
 * 1.ik分词器，先导依赖
 * 2.new的时候第二个参数为是否开启智能模式，false不开启会将词拆的比较散
 *
 */
public class KeywordUtil {
    public static List<String> analyze(String keyword){
        ArrayList<String> result = new ArrayList<>();
        StringReader reader = new StringReader(keyword);
        //填写两个参数，第二个参数为是否开启智能模式
        IKSegmenter ikSegmenter = new IKSegmenter(reader, true);
        Lexeme next = null;
        try {
            while ((next=ikSegmenter.next()) != null){
                result.add(next.getLexemeText());
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        return result;
    }


    public static void main(String[] args) {
        String s = "小米智能手机";
        System.out.println(analyze(s));
    }
}
