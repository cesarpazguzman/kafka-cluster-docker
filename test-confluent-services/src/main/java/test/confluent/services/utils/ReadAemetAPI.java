package test.confluent.services.utils;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public class ReadAemetAPI {

    public static GenericRecord newRecord(Schema schema){
        GenericRecord record = new GenericData.Record(schema);
        record.put("id_ema", 123);
        record.put("ts_measurement", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
                .format(Calendar.getInstance().getTime()));
        record.put("locality", "Santiago");
        record.put("Temperature", 20.5);
        record.put("Rain", 0.0);
        record.put("Humidity", 0.0);
        record.put("Cloudy", 0.0);

        return record;
    }
}
