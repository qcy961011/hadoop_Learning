package com.hive.udaf;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.AbstractGenericUDAFResolver;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.serde2.lazy.LazyInteger;
import org.apache.hadoop.hive.serde2.lazy.LazyLong;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;


/**
 * 自定义sum函数， 实现多行求合
 */
public class SumUDAF extends AbstractGenericUDAFResolver {

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
        return new SumEvaluator();
    }

    /**
     * UDAF 函数算子类 ， 在此类里实现函数逻辑
     */
    public static class SumEvaluator extends GenericUDAFEvaluator {

        /**
         * 装中间结果的bean类
         */
        public static class SumAgg extends AbstractAggregationBuffer {
            long sum = 0L;

            public long getSum() {
                return sum;
            }

            public void setSum(long sum) {
                this.sum = sum;
            }
        }


        @Override
        public ObjectInspector init(Mode m, ObjectInspector[] parameters) throws HiveException {
            super.init(m, parameters);

            // 根据不同阶段返回不同的数据类型
            // 因为sum() 求在所有阶段的输出都是Long类型，所以不需要区分阶段去判断
            return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        }

        @Override
        public AggregationBuffer getNewAggregationBuffer() throws HiveException {
            AggregationBuffer bean = new SumAgg();
            return bean;
        }

        @Override
        public void reset(AggregationBuffer agg) throws HiveException {
            SumAgg sumAgg = (SumAgg) agg;
            sumAgg.setSum(0L);
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
            SumAgg sumAgg = (SumAgg) agg;
            Object inputObj = parameters[0];
            IntWritable inputLong = null;
            if (inputObj instanceof LazyInteger) {
                LazyInteger lazyLong = (LazyInteger) inputObj;
                inputLong = lazyLong.getWritableObject();
            } else if (inputObj instanceof IntWritable) {
                inputLong = (IntWritable) inputObj;
            }
            long num = inputLong.get();
            sumAgg.setSum(sumAgg.getSum() + num);
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
            SumAgg sumAgg = (SumAgg) agg;
            long sum = sumAgg.getSum();
            LongWritable longWritable = new LongWritable();
            longWritable.set(sum);
            return longWritable;
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
            SumAgg sumAgg = (SumAgg) agg;
            LongWritable longWritable = null;
            if (partial instanceof LazyLong) {
                LazyLong lazyLong = (LazyLong) partial;
                longWritable = lazyLong.getWritableObject();
            } else if (partial instanceof LongWritable) {
                longWritable = (LongWritable) partial;
            }
            long num = longWritable.get();
            sumAgg.setSum(sumAgg.getSum() + num);
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
            SumAgg sumAgg = (SumAgg) agg;
            long sum = sumAgg.getSum();
            LongWritable longWritable = new LongWritable();
            longWritable.set(sum);
            return longWritable;
        }
    }

}
