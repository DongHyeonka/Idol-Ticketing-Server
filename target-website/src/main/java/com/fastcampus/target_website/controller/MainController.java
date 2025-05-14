package com.fastcampus.target_website.controller;

import java.util.Arrays;

import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.client.RestTemplate;
import org.springframework.web.util.UriComponentsBuilder;

import jakarta.servlet.http.Cookie;
import jakarta.servlet.http.HttpServletRequest;

@Controller
public class MainController {
    RestTemplate restTemplate = new RestTemplate();
    
    @GetMapping("/")
    public String index(
        @RequestParam(name = "queue", defaultValue = "default") String queue,
        @RequestParam(name = "user_id") Long userId,
        HttpServletRequest request
    ) {
        var cookies = request.getCookies();
        var cookieName = "user-queue-%s-token".formatted(queue);

        String token = "";
        if (cookies != null) {
            var cookie = Arrays.stream(cookies).filter(i -> i.getName().equalsIgnoreCase(cookieName)).findFirst();
            token = cookie.orElse(new Cookie(cookieName, "")).getValue();
        }

        var uri = UriComponentsBuilder
                .fromUriString("http://127.0.0.1:9010")
                .path("/api/v1/queue/allowed")
                .queryParam("queue", queue)
                .queryParam("user_id", userId)
                .queryParam("token", token)
                .encode()
                .build()
                .toUri();

        ResponseEntity<AllowedUserResponse> response = restTemplate.getForEntity(uri, AllowedUserResponse.class);
        if (response.getBody() == null || !response.getBody().allowed()) {
            // 대기 웹페이지로 리다이렉트
            return "redirect:http://127.0.0.1:9010/waiting-room?user_id=%d&redirect_url=%s".formatted(
                    userId, "http://127.0.0.1:9000?user_id=%d".formatted(userId));
        }
        // 허용 상태라면 해당 페이지를 진입
        return "index";
    }

    public record AllowedUserResponse(Boolean allowed) {
    }
}
