package ru.pitaza170.newsaggregator;

import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api")
public class FirstRestController {

    @GetMapping("/hello")
    public String hello() {
        return "hello";
    }

}
