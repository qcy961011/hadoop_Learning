package com.hive.udf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.lazy.LazyString;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

import java.io.*;
import java.util.HashMap;
import java.util.Map;

/**
 * 将国家码转为国家名称的UDF函数
 */

public class CountryCodeToCountryNameUDF extends GenericUDF {

    public static Map<String, String> countryMap = new HashMap<>();

    static {
        try (BufferedReader bufferedReader = new BufferedReader(new InputStreamReader(new FileInputStream(new File("D:/data/dic.txt"))))) {
            String line = null;
            while ((line = bufferedReader.readLine()) != null) {
                String[] strarr = line.split("\t");
                String code = strarr[0];
                String name = strarr[1];
                countryMap.put(code, name);
            }
            System.out.println("MapSize :" + countryMap.size());
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    /**
     * 初始化， 做数据校验
     *
     * @param objectInspectors
     * @return
     * @throws UDFArgumentException
     */
    @Override
    public ObjectInspector initialize(ObjectInspector[] objectInspectors) throws UDFArgumentException {
        // 校验输入参数个数
        if (objectInspectors.length != 1) {
            throw new UDFArgumentException("input param must one");
        }

        // 校验输入参数是什么类型
        if (!objectInspectors[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentException("input param Category must PRIMITIVE");
        } else {

        }
        // 校验输入参数是的指定类型的什么子类型
        if (!objectInspectors[0].getTypeName().equalsIgnoreCase(PrimitiveObjectInspector.PrimitiveCategory.STRING.name())) {
            throw new UDFArgumentException("input param Primitive Category type must String");
        }

        // 返回函数输出的对象 , 类型是字符串
        return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    }


    /**
     * 实际功能实现
     *
     * @param deferredObjects
     * @return
     * @throws HiveException
     */
    @Override
    public Object evaluate(DeferredObject[] deferredObjects) throws HiveException {
        Object obj = deferredObjects[0].get();

        Text inputString = null;

        if (obj instanceof LazyString) {
            LazyString lz = (LazyString) obj;
            inputString = lz.getWritableObject();
        } else if(obj instanceof Text) {
            inputString = (Text) obj;
        }

        String doce = inputString.toString();
        String name = countryMap.get(doce);
        name = name == null ? "小国家" : name;
        return new Text(name);
    }

    @Override
    public String getDisplayString(String[] strings) {
        return null;
    }

    public static void main(String[] args) {
        CountryCodeToCountryNameUDF countryCodeToCountryNameUDF = new CountryCodeToCountryNameUDF();
    }
}
