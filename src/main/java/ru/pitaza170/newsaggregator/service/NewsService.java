package ru.pitaza170.newsaggregator.service;

import lombok.SneakyThrows;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.multipart.MultipartFile;
import ru.pitaza170.newsaggregator.model.News;
import ru.pitaza170.newsaggregator.repository.NewsRepository;
import ru.pitaza170.newsaggregator.util.CSVHelper;

import javax.persistence.EntityNotFoundException;
import java.io.IOException;
import java.util.List;

@Service
public class NewsService {

    private final NewsRepository newsRepository;
    @Autowired
    public NewsService(NewsRepository newsRepository) {
        this.newsRepository = newsRepository;
    }

    public List<News> findAll() {
        return newsRepository.findAll();
    }

    @SneakyThrows
    public void create(MultipartFile file) throws EntityNotFoundException {
        try {
            List<News> tutorials = CSVHelper.csvToNews(file.getInputStream());
            newsRepository.saveAll(tutorials);
        } catch (IOException e) {
            throw new RuntimeException("fail to store csv data: " + e.getMessage());
        }
    }

    public void deleteAll() {
        newsRepository.deleteAll();
    }

    public List<News> findByRole(String role) {
       return newsRepository.findByRole(role);
    }
}
