package ru.pitaza170.newsaggregator.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;
import ru.pitaza170.newsaggregator.model.News;
import ru.pitaza170.newsaggregator.repository.NewsRepository;

import java.util.List;

@Service
@Transactional(readOnly = true)
public class NewsService {

    private final NewsRepository newsRepository;

    @Autowired
    public NewsService(NewsRepository newsRepository) {
        this.newsRepository = newsRepository;
    }

    public List<News> findAll() {
        return newsRepository.findAll();
    }
}
