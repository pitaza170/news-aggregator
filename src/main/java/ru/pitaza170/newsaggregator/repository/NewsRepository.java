package ru.pitaza170.newsaggregator.repository;

import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.stereotype.Repository;
import ru.pitaza170.newsaggregator.model.News;

@Repository
public interface NewsRepository extends JpaRepository<News, Integer> {
}
