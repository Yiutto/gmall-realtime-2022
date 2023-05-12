package com.atguigu.util;

import com.ctc.wstx.sr.ReaderCreator;
import org.wltea.analyzer.core.IKSegmenter;
import org.wltea.analyzer.core.Lexeme;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.List;

public class KeywordUtil {
    public static List<String> splitKeyword (String keyword) throws IOException {
        // 创建集合，用于存放切分后的数据
        ArrayList<String> list = new ArrayList<>();

        StringReader reader = new StringReader(keyword);

        // 创建IK分词器  ik_smart(true)  ik_max_word(false)
        IKSegmenter ikSegmenter = new IKSegmenter(reader, false);

        // 循环取出切分好的词
        Lexeme next = ikSegmenter.next();
        while (next != null) {
            String word = next.getLexemeText();
            list.add(word);
            next = ikSegmenter.next();
        }
        // 最终返回集合
        return list;
    }

    public static void main(String[] args) throws IOException {
        System.out.println(splitKeyword("大数据之Flink学习"));
    }
}
