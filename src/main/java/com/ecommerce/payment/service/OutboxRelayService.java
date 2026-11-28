package com.ecommerce.payment.service;

import com.ecommerce.payment.entity.OutboxEvent;
import com.ecommerce.payment.entity.OutboxStatus;
import com.ecommerce.payment.repository.OutboxEventRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageBuilder;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.util.List;

@Slf4j
@Service
@RequiredArgsConstructor
public class OutboxRelayService {

    private static final int MAX_RETRY_COUNT = 5;

    private final OutboxEventRepository outboxEventRepository;
    private final RabbitTemplate rabbitTemplate;

    @Scheduled(fixedDelay = 2000)
    @Transactional
    public void publishPendingEvents() {
        List<OutboxEvent> pendingEvents = outboxEventRepository
                .findTop50ByStatusOrderByCreatedAtAsc(OutboxStatus.PENDING);

        if (pendingEvents.isEmpty()) {
            return;
        }

        for (OutboxEvent event : pendingEvents) {
            try {
                Message message = MessageBuilder
                        .withBody(event.getPayload().getBytes(StandardCharsets.UTF_8))
                        .setContentType(MessageProperties.CONTENT_TYPE_JSON)
                        .setContentEncoding(StandardCharsets.UTF_8.name())
                        .setHeader("__TypeId__", event.getEventType())
                        .build();

                rabbitTemplate.send(event.getExchangeName(), event.getRoutingKey(), message);

                event.setStatus(OutboxStatus.PUBLISHED);
                event.setPublishedAt(LocalDateTime.now());
                event.setLastError(null);
                outboxEventRepository.save(event);

                log.info("Outbox event published. id={}, routingKey={}, aggregateId={}",
                        event.getId(), event.getRoutingKey(), event.getAggregateId());

            } catch (Exception ex) {
                int nextRetry = event.getRetryCount() + 1;
                event.setRetryCount(nextRetry);
                event.setLastError(ex.getMessage());

                if (nextRetry >= MAX_RETRY_COUNT) {
                    event.setStatus(OutboxStatus.FAILED);
                    log.error("Outbox event permanently failed after {} attempts. id={}, routingKey={}",
                            nextRetry, event.getId(), event.getRoutingKey(), ex);
                } else {
                    log.warn("Outbox event publish failed (attempt {}/{}). id={}",
                            nextRetry, MAX_RETRY_COUNT, event.getId(), ex);
                }
                outboxEventRepository.save(event);
            }
        }
    }
}