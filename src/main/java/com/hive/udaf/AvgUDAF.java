package com.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazybinary.LazyBinaryStruct;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.IntWritable;

import java.util.ArrayList;
import java.util.List;

public class AvgUDAF extends AbstractGenericUDAFResolver {

    @Override
    public GenericUDAFEvaluator getEvaluator(TypeInfo[] info) throws SemanticException {
        // 校验函数的输入参数
        if (info.length != 1) {
            throw new UDFArgumentException("input param must one");
        }
        // 校验输入参数是什么类型
        if (!info[0].getCategory().equals(ObjectInspector.Category.PRIMITIVE)) {
            throw new UDFArgumentException("input param Category must PRIMITIVE");
        } else {

        }
        // 校验输入参数是的指定类型的什么子类型
        if (info[0].getTypeName().equalsIgnoreCase(PrimitiveObjectInspector.PrimitiveCategory.LONG.name())) {
            throw new UDFArgumentException("input param Primitive Category type must Long");
        }
        return new AvgEvaluator();
    }

    /**
     * UDAF 函数算子类 ， 在此类里实现函数逻辑
     */
    public static class AvgEvaluator extends GenericUDAFEvaluator {

        /**
         * 装中间结果的bean类
         */
        public static class AvgAgg extends AbstractAggregationBuffer {

            /**
             *  求合
             */
            int sum = 0;

            /**
             *  计数
             */
            int count = 0;

            public int getSum() {
                return sum;
            }

            public void setSum(int sum) {
                this.sum = sum;
            }

            public int getCount() {
                return count;
            }

            public void setCount(int count) {
                this.count = count;
            }
        }


        @Override
        public ObjectInspector init(GenericUDAFEvaluator.Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            if (m == Mode.PARTIAL1 || m == Mode.PARTIAL2) {
                // 输出sum 和 count

                // 复合类型的名称列表
                List<String> structFieldNames = new ArrayList<>();
                structFieldNames.add("sum");
                structFieldNames.add("count");
                // 复合类型中名称对应的类型列表
                List<ObjectInspector> structFieldObjectInspectors = new ArrayList<>();
                structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);
                structFieldObjectInspectors.add(PrimitiveObjectInspectorFactory.writableIntObjectInspector);

                return ObjectInspectorFactory.getStandardStructObjectInspector(structFieldNames , structFieldObjectInspectors);
            }


            // 根据不同阶段返回不同的数据类型
            // 因为sum() 求在所有阶段的输出都是Long类型，所以不需要区分阶段去判断
            return PrimitiveObjectInspectorFactory.writableDoubleObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AvgAgg bean = new AvgAgg();
            return bean;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            AvgAgg avgAgg = (AvgAgg) agg;
            avgAgg.setSum(0);
            avgAgg.setCount(0);
        }


        /**
         * 读一行数据进行求合，将求和的中间结果放到bean对象中
         *
         * @param agg
         * @param parameters
         * @throws HiveException
         */
        @Override
        public void iterate(AggregationBuffer agg, Object[] parameters) throws HiveException {
            AvgAgg avgAgg = (AvgAgg) agg;
            Object inputObj = parameters[0];
            IntWritable inputLong = null;
            if (inputObj instanceof LazyInteger) {
                LazyInteger lazyLong = (LazyInteger) inputObj;
                inputLong = lazyLong.getWritableObject();
            } else if (inputObj instanceof IntWritable) {
                inputLong = (IntWritable) inputObj;
            }
            int num = inputLong.get();
            avgAgg.setSum(avgAgg.getSum() + num);
            avgAgg.setCount(avgAgg.getCount() + 1);
        }


        /**
         * 将bean对象中的数据，转化成mapreduce间传输的数据类型
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminatePartial(AggregationBuffer agg) throws HiveException {
            AvgAgg avgAgg = (AvgAgg) agg;
            int sum = avgAgg.getSum();
            int count = avgAgg.getCount();
            Object[] transferObj = {new IntWritable() , new IntWritable()};
            ((IntWritable) transferObj[0]).set(sum);
            ((IntWritable) transferObj[1]).set(count);
            return transferObj;
        }

        /**
         * 将map阶段传输过来的数据进行合并，将合并结果放到beean对象中
         *
         * @param agg
         * @param partial
         * @throws HiveException
         */
        @Override
        public void merge(AggregationBuffer agg, Object partial) throws HiveException {
            AvgAgg avgAgg = (AvgAgg) agg;
            IntWritable sumparam = null;
            IntWritable countparam = null;
            if (partial instanceof LazyBinaryStruct) {
                LazyBinaryStruct lz = (LazyBinaryStruct) partial;
                sumparam = (IntWritable) lz.getField(0);
                countparam = (IntWritable) lz.getField(1);
            }
            int num = sumparam.get();
            int count = countparam.get();
            avgAgg.setSum(avgAgg.getSum() + num);
            avgAgg.setCount(avgAgg.getCount() + count);
        }

        /**
         * 将bean对象中的数据，转化成mapreduce输出的数据类型
         *
         * @param agg
         * @return
         * @throws HiveException
         */
        @Override
        public Object terminate(AggregationBuffer agg) throws HiveException {
            AvgAgg avgAgg = (AvgAgg) agg;
            int sum = avgAgg.getSum();
            int count = avgAgg.getCount();
            double avg = (double) sum / count;
            DoubleWritable output = new DoubleWritable(avg);
            return output;
        }
    }

}
