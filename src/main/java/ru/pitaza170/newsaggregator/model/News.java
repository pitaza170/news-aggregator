package ru.pitaza170.newsaggregator.model;

import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

import javax.persistence.*;

@Entity
@Getter
@Setter
@NoArgsConstructor
@Table(name="data")
public class News {
    @Id
    @Column
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private int id;

    @Column(name="links")
    private String links;

    @Column(name = "title")
    private String title;

    @Column(name="tags")
    private String tags;

    @Column(name="text")
    private String text;

    @Column(name="date")
    private String date;


}
