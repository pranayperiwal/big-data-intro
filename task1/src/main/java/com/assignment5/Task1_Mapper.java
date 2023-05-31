package com.assignment5;

import java.io.IOException;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class Task1_Mapper extends MapReduceBase implements Mapper<LongWritable,Text,Text,Text>{
    public void map(LongWritable key, Text value,OutputCollector<Text,Text> output,
                    Reporter reporter) throws IOException{
        String line = value.toString();

        // skip the line containing the header of the columns
        if(line.contains("queryTime")){
            return;
        }

        String transformedLine = transformLine(line);

        output.collect(new Text(line), new Text(transformedLine));

    }


    public String transformLine(String line){
        String[] splitLine = line.split("\\s+");
        String url = splitLine[2];
        if(url.contains("/")){
            url = url.substring(0,url.indexOf('/'));
        }
        if(url.indexOf('"')==0){
            url = url.substring(1);
        }

        return splitLine[0] + " " + splitLine[1] + " " + url;

    }

}