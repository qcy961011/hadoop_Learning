package com.qiao.util;

import com.util.JsonUtil;
import org.junit.Test;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.util.Map;

public class UtilTest {

    @Test
    public void testJson() throws Exception {

        BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("data/test/onlinelog"))));

        String line = null;
        while ((line = reader.readLine()) != null){
            line = line.replace("\\" , "");
            Map map = JsonUtil.readJson(line);
            Map uo = (Map) map.get("uo");
            System.out.println(uo.get("uid"));
        }

//        String log = "{\"uo\":{\"uid\":\"085100012449\",\"step\":10,\"sid\":\"00000200001B50800001B4014201CA7B\",\"ti\":\"推荐\",\"pid\":\"1\"},\"bs\":{\"support\":{\"Au\":\"object\",\"Lo\":\"object\",\"Se\":\"object\",\"P\":\"undefined\"},\"sid\":\"S65_M2\",\"ac\":\"851\",\"en\":\"GZXMT\",\"gid\":\"-1\",\"epf\":\"1\",\"operators\":\"联通\"}}";
//        Map map = JsonUtil.readJson(log);
//        Map uo = (Map) map.get("uo");
//        System.out.println(uo.get("uid"));
    }
}
