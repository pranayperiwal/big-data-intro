package com.assignment5;

import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class Task1_Reducer  extends MapReduceBase implements Reducer<Text,Text,Text,Text> {
    public void reduce(Text key, Iterator<Text> values,OutputCollector<Text,Text> output,
                       Reporter reporter) throws IOException {

        while(values.hasNext()){
            output.collect(new Text("Result"), new Text(values.next()));
        }

    }
}