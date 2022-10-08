package ru.pitaza170.newsaggregator.util;

import org.apache.commons.csv.*;
import ru.pitaza170.newsaggregator.model.News;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import static ru.pitaza170.newsaggregator.common.Constants.FAILED_TO_PARSE;

public class CSVHelper {

    public static List<News> csvToNews(InputStream is) {
        try (BufferedReader fileReader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8));

             CSVParser csvParser = new CSVParser(fileReader,
                     CSVFormat.DEFAULT.withFirstRecordAsHeader().withIgnoreHeaderCase().withTrim().withHeader());) {

            List<News> newsList = new ArrayList<>();
            Iterable<CSVRecord> csvRecords = csvParser.getRecords();

            for (CSVRecord rec : csvRecords) {
                News news = new News(
                        Long.parseLong(rec.get("id")),
                        rec.get("links"),
                        rec.get("title"),
                        rec.get("role"),
                        rec.get("text")
                );
                newsList.add(news);
            }

            return newsList;
        } catch (IOException e) {
            throw new RuntimeException(FAILED_TO_PARSE + e.getMessage());
        }
    }

}
