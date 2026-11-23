package dev.vality.file.storage.util;

public class ContentDispositionUtil {

    public static String inline(String fileName) {
        String escapedFileName = fileName
                .replace("\\", "\\\\")
                .replace("\"", "\\\"");
        return "inline; filename=\"" + escapedFileName + "\"";
    }
}
