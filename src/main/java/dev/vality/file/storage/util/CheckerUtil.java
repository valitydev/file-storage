package dev.vality.file.storage.util;

import org.springframework.util.StringUtils;

public class CheckerUtil {

    public static void checkString(String string, String exMessage) {
        if (!StringUtils.hasText(string) || string.contains(" ")) {
            throw new IllegalArgumentException(exMessage);
        }
    }
}
