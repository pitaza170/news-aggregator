package ru.pitaza170.newsaggregator.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import ru.pitaza170.newsaggregator.model.News;
import ru.pitaza170.newsaggregator.service.NewsService;

import java.util.List;

@RestController
@RequestMapping("/api/v1/feed")
public class NewsController {

    private final NewsService newsService;

    @Autowired
    public NewsController(NewsService newsService) {
        this.newsService = newsService;
    }

    @GetMapping()
    public List<News> getNews() {
        return newsService.findAll();
    }

}
