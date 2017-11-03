import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.chain.ChainMapper;
import org.apache.hadoop.mapreduce.lib.input.MultipleInputs;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class UnitMultiplication {

    public static class TransitionMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: fromPage\t toPage1,toPage2,toPage3
            //target: build transition matrix unit -> fromPage\t toPage=probability
            String[] outputkey_nexts= value.toString().trim().split("\t");
            if(outputkey_nexts.length<2) {
                return;
            }

            String outputkey = outputkey_nexts[0].trim();
            String[] nexts = outputkey_nexts[1].trim().split(",");
            double weight = (double) 1/nexts.length;
            for(String next: nexts){
                String outputvalue = next + "=" + weight;
                context.write(new Text(outputkey),new Text(outputvalue));
            }
        }
    }

    public static class PRMapper extends Mapper<Object, Text, Text, Text> {

        @Override
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {

            //input format: Page\t PageRank
            //target: write to reducer
            String[] outputkey_curr = value.toString().trim().split("\\s+");
            if(outputkey_curr.length<2){
                return;
            }
            String outputkey = outputkey_curr[0].trim();
            String curr = outputkey_curr[1].trim();
            context.write(new Text(outputkey),new Text(curr));
        }
    }

    public static class MultiplicationReducer extends Reducer<Text, Text, Text, Text> {

        class Pair{
            double weight;
            String next;

            public Pair(double weight, String next){
                this.weight = weight;
                this.next = next;
            }
        }

        float beta;

        public void setup(Context context){
             Configuration configuration = context.getConfiguration();
             beta = Float.valueOf(configuration.get("beta"));
        }


        @Override
        public void reduce(Text key, Iterable<Text> values, Context context)
                throws IOException, InterruptedException {

            //input key = fromPage value=<toPage=probability..., pageRank>
            //target: get the unit multiplication
            List<Pair> pairs = new ArrayList<Pair>();
            double curr = 0;

            for(Text val :values){
                if(val.toString().contains("=")){
                    String[] tmp = val.toString().trim().split("=");
                    String next = tmp[0].trim();
                    double weight = Double.parseDouble(tmp[1].trim());
                    pairs.add(new Pair(weight,next));
                }else {
                    curr = Double.parseDouble(val.toString().trim());
                }
            }

            for(Pair pair: pairs){
                double next_weight = (1-beta)*pair.weight * curr;
                context.write(new Text(pair.next), new Text(String.valueOf(next_weight)));
            }

            context.write(key, new Text(String.valueOf(beta * curr)));

        }
    }

    public static void main(String[] args) throws Exception {

        Configuration conf = new Configuration();
        conf.set("beta", args[3]);
        Job job = Job.getInstance(conf);
        job.setJarByClass(UnitMultiplication.class);

        //how chain two mapper classes?
        ChainMapper.addMapper(job, TransitionMapper.class, Object.class, Text.class, Text.class, Text.class, conf);
        ChainMapper.addMapper(job, PRMapper.class, Object.class, Text.class, Text.class, Text.class, conf);

        job.setReducerClass(MultiplicationReducer.class);

        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        MultipleInputs.addInputPath(job, new Path(args[0]), TextInputFormat.class, TransitionMapper.class);
        MultipleInputs.addInputPath(job, new Path(args[1]), TextInputFormat.class, PRMapper.class);

        FileOutputFormat.setOutputPath(job, new Path(args[2]));
        job.waitForCompletion(true);
    }

}
