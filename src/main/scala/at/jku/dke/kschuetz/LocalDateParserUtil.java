package at.jku.dke.kschuetz;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public class LocalDateParserUtil {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss");
    private static final String formatString2 = "yyyy-MM-dd'T'HH:mm:ss";
    private static final DateTimeFormatter formatter2 = DateTimeFormatter.ofPattern(formatString2);
    private static final List<DateTimeFormatter> formatters = new ArrayList<>();
    static {
        formatters.add(formatter);
        formatters.add(formatter2);
    }

    private LocalDateParserUtil(){}

    public static LocalDate parseLocalDateEnronVersion(String dateString){
        dateString = dateString.trim();
        if(dateString.charAt(6) == ' ')
            dateString = dateString.substring(0, 5) + "0" + dateString.substring(5);
        if(dateString.length() > 25) dateString = dateString.substring(0, 25);
        for(DateTimeFormatter f : formatters){
            try{
                return LocalDate.parse(dateString, f);
            }catch(DateTimeParseException ignore){
            }
        }
        throw new DateTimeParseException("Failed parsing date: " + dateString, dateString, -1);
    }

    public static LocalDate parseLocalDateStreamVersion(String dateString){
        dateString = dateString.trim();
        if(dateString.length() > 19) dateString = dateString.substring(0, 19);
        return LocalDate.parse(dateString, formatter2);
    }
}
