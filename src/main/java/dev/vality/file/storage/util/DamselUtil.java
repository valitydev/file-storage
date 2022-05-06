package dev.vality.file.storage.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.vality.geck.serializer.kit.json.JsonHandler;
import dev.vality.geck.serializer.kit.json.JsonProcessor;
import dev.vality.geck.serializer.kit.tbase.TBaseHandler;
import dev.vality.geck.serializer.kit.tbase.TBaseProcessor;
import org.apache.thrift.TBase;

import java.io.IOException;

public class DamselUtil {

    public static String toJsonString(TBase value) {
        return toJson(value).toString();
    }

    public static JsonNode toJson(TBase value) {
        try {
            return new TBaseProcessor().process(value, new JsonHandler());
        } catch (IOException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

    public static <T extends TBase> T fromJson(String jsonString, Class<T> type) {
        try {
            return new JsonProcessor().process(new ObjectMapper().readTree(jsonString), new TBaseHandler<>(type));
        } catch (IOException ex) {
            throw new IllegalArgumentException(ex);
        }
    }

}
