import org.apache.kafka.clients.producer.ProducerConfig;
//import org.apache.kafka.clients.producer.KafkaProducer;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class KafkaProducer {
    public static void main (String[] args){
        //step -1 set the properties
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG,"producer-id1");
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092,localhost:9093");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        //step -2 create object of kafka producer

        //KafkaProducer<Integer, String> producer = new KafkaProducer<Integer, String>(props);
        org.apache.kafka.clients.producer.KafkaProducer<Integer,String> producer = new org.apache.kafka.clients.producer.KafkaProducer<Integer,String>(props);

        // step- 3 calling the send method on producer object

        producer.send(new ProducerRecord<Integer,String>("our new topic",1,"THIS IS MY FIRST MESSAGE"));

        // STEP 4 CLOSE THE PRODUCER OBJECT

        producer.close();





















    }
























}

