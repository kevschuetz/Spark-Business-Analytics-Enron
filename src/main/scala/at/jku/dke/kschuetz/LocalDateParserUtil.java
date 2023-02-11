package at.jku.dke.kschuetz;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.ArrayList;
import java.util.List;

public class LocalDateParserUtil {
    private static final DateTimeFormatter formatter = DateTimeFormatter.ofPattern("EEE, dd MMM yyyy HH:mm:ss");
    private static final List<DateTimeFormatter> formatters = new ArrayList<>();
    static {
        formatters.add(formatter);
    }

    private LocalDateParserUtil(){}

    public static LocalDate parseLocalDate(String dateString){
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
}
