package com.fastcampus.flow.service;

import java.time.Instant;

import org.springframework.data.redis.core.ReactiveRedisTemplate;
import org.springframework.stereotype.Service;

import com.fastcampus.flow.exception.ErrorCode;

import lombok.RequiredArgsConstructor;
import reactor.core.publisher.Mono;

@Service
@RequiredArgsConstructor
public class UserQueueService {
    private final ReactiveRedisTemplate<String, String> reactiveRedisTemplate;
    private final String USER_QUEUE_WAIT_KEY = "users:queue:%s:wait";
    private final String USER_QUEUE_PROCEED_KEY = "users:queue:%s:proceed";

    public Mono<Long> registerWaitQueue(final String queue, final Long userId) {
        var unixTimestamp = Instant.now().getEpochSecond(); // 여기서 현재 시간을 초 단위로 가져온다.
        return reactiveRedisTemplate.opsForZSet()
                .add(USER_QUEUE_WAIT_KEY.formatted(queue), userId.toString(), unixTimestamp) // 이 시점에서는 true/false를 반환하게 되는데 3번째 인자는 스코어 값이 double로 받기 때문에
                .filter(i -> i) // 성공한 경우에대해서만 filter
                .switchIfEmpty(Mono.error(ErrorCode.QUEUE_ALREADY_REGISTERED_USER.build())) // 실패한 경우에는 예외 처리
                .flatMap(i -> reactiveRedisTemplate.opsForZSet().rank(USER_QUEUE_WAIT_KEY.formatted(queue), userId.toString())) // 성공에 대한 내용에 대해서 랭크를 가져오고
                .map(i -> i >= 0 ? i + 1 : i); // 랭크가 0보다 크거나 같으면 1을 더해주고, 아니면 그대로 반환
    }

    public Mono<Long> allowUser(final String queue, final Long count) {
        return reactiveRedisTemplate.opsForZSet().popMin(USER_QUEUE_WAIT_KEY.formatted(queue), count)
                .flatMap(member -> reactiveRedisTemplate.opsForZSet().add(
                    USER_QUEUE_PROCEED_KEY.formatted(queue),
                    member.getValue(), Instant.now().getEpochSecond()
                ))
                .count();
    }

    public Mono<Boolean> isAllowed(final String queue, final Long userId) {
        return reactiveRedisTemplate.opsForZSet().rank(USER_QUEUE_PROCEED_KEY.formatted(queue), userId.toString())
                .defaultIfEmpty(-1L)
                .map(rank -> rank >= 0);
    }
}
