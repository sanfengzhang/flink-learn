package com.han.kafka;

import com.sun.xml.internal.messaging.saaj.packaging.mime.util.ASCIIUtility;
import org.apache.commons.io.IOUtils;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Properties;

public class Produce {

    @Test
    public void testCharr() throws Exception {
//        String s = IOUtils.toString(new FileInputStream(new File("C:\\Users\\hanlin01\\Desktop\\新建文本文档 (3).log")), "UTF-8");
//        System.out.println(s);


        System.out.println(new String("\u8fd9\u662f\u4e00\u4e2a\u4f8b\u5b50".getBytes(), "utf-8"));
        //  System.out.println(toString(aa,0,aa.length));


    }


    public static byte[] getBytes(String str) {
        char[] toCharArray = str.toCharArray();
        int length = toCharArray.length;
        byte[] bArr = new byte[length];
        int i = 0;
        while (i < length) {
            int i2 = i + 1;
            bArr[i] = (byte) toCharArray[i];
            i = i2;
        }
        return bArr;
    }

    public static String toString(byte[] bArr, int i, int i2) {
        int i3 = i2 - i;
        char[] cArr = new char[i3];
        int i4 = 0;
        while (i4 < i3) {
            int i5 = i4 + 1;
            int i6 = i + 1;
            cArr[i4] = (char) (bArr[i] & 255);
            i4 = i5;
            i = i6;
        }
        return new String(cArr);
    }


    public static String decodeUnicode(final String dataStr) {
        int start = 0;
        int end = 0;
        final StringBuffer buffer = new StringBuffer();
        while (start > -1) {
            end = dataStr.indexOf("\\u", start + 2);
            String charStr = "";
            if (end == -1) {
                charStr = dataStr.substring(start + 2, dataStr.length());
            } else {
                charStr = dataStr.substring(start + 2, end);
            }
            char letter = (char) Integer.parseInt(charStr, 16);
            buffer.append(Character.toString(letter));
            start = end;
        }
        return buffer.toString();
    }

    private List<String> getWeiboData() throws IOException {

        FileInputStream fis = new FileInputStream(new File("D:\\dataset\\Weibo_Data\\weibo_train_data.txt"));
        List<String> _list = IOUtils.readLines(fis, "UTF-8");

        fis.close();
        return _list;

    }

    @Test
    public void produceWeiBoData() throws Exception {

        Properties props = getProperties();

        List<String> _list = getWeiboData();

        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (String weibo : _list) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("weibo_topic", weibo);
            producer.send(record, new Callback() {

                public void onCompletion(RecordMetadata recordMetadata, Exception arg1) {
                    if (arg1 != null) {
                        System.out.println(arg1);
                    }
                }
            });
            Thread.sleep(1000);
        }

        producer.close();

    }

    private List<String> getWeiboData10() throws IOException {

        FileInputStream fis = new FileInputStream(new File("C:\\Users\\hanlin01\\Desktop\\leader.txt"));
        List<String> _list = IOUtils.readLines(fis, "UTF-8");

        fis.close();
        return _list;

    }

    @Test
    public void produceWeiBoData10() throws FileNotFoundException, IOException {
        Properties props = getProperties();
        List<String> _list = getWeiboData();
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(props);
        for (String weibo : _list) {
            ProducerRecord<String, String> record = new ProducerRecord<String, String>("test-leader", weibo);
            producer.send(record, new Callback() {

                public void onCompletion(RecordMetadata recordMetadata, Exception arg1) {
                    if (arg1 != null) {
                        System.out.println(arg1);
                    }

                }
            });
        }
        System.out.println("----------------end---------------");
        producer.close();
    }

    private Properties getProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.12.100:9092,192.168.12.101:9092,192.168.12.102:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 10000);
        props.put("linger.ms", 10);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class);
        props.put("value.serializer", StringSerializer.class);

        return props;
    }

}
