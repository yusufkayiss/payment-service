package com.ecommerce.payment.service;

import com.ecommerce.payment.config.RabbitMQConfig;
import com.ecommerce.payment.dto.event.OrderCreatedEvent;
import com.ecommerce.payment.dto.event.PaymentCompletedEvent;
import com.ecommerce.payment.dto.event.PaymentFailedEvent;
import com.ecommerce.payment.entity.OutboxEvent;
import com.ecommerce.payment.entity.OutboxStatus;
import com.ecommerce.payment.entity.Payment;
import com.ecommerce.payment.entity.PaymentStatus;
import com.ecommerce.payment.repository.OutboxEventRepository;
import com.ecommerce.payment.repository.PaymentRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Service
@RequiredArgsConstructor
public class PaymentService {

    private static final BigDecimal MAX_LIMIT = new BigDecimal("10000.00");

    private final PaymentRepository paymentRepository;
    private final OutboxEventRepository outboxEventRepository;
    private final ObjectMapper objectMapper;

    @Transactional
    public void processOrderPayment(OrderCreatedEvent event) {
        if (paymentRepository.existsByOrderId(event.getOrderId())) {
            log.warn("Duplicate OrderCreatedEvent ignored for orderId={}", event.getOrderId());
            return;
        }

        boolean isApproved = isPaymentApproved(event.getTotalAmount());

        if (isApproved) {
            String providerRef = "PAY-" + UUID.randomUUID().toString().substring(0, 8).toUpperCase();

            Payment payment = Payment.builder()
                    .orderId(event.getOrderId())
                    .userEmail(event.getUserEmail())
                    .amount(event.getTotalAmount())
                    .status(PaymentStatus.COMPLETED)
                    .providerReference(providerRef)
                    .build();

            Payment savedPayment = paymentRepository.save(payment);

            PaymentCompletedEvent completedEvent = PaymentCompletedEvent.builder()
                    .paymentId(savedPayment.getId())
                    .orderId(savedPayment.getOrderId())
                    .userEmail(savedPayment.getUserEmail())
                    .amount(savedPayment.getAmount())
                    .providerReference(providerRef)
                    .completedAt(LocalDateTime.now())
                    .build();

            saveOutboxEvent(
                    savedPayment.getId().toString(),
                    "PaymentCompletedEvent",
                    RabbitMQConfig.PAYMENT_COMPLETED_ROUTING_KEY,
                    completedEvent
            );

            log.info("Payment COMPLETED for orderId={}, paymentId={}, ref={}",
                    savedPayment.getOrderId(), savedPayment.getId(), providerRef);
        } else {
            String reason = "Payment declined: invalid amount or limit exceeded (" + event.getTotalAmount() + ")";

            Payment payment = Payment.builder()
                    .orderId(event.getOrderId())
                    .userEmail(event.getUserEmail())
                    .amount(event.getTotalAmount())
                    .status(PaymentStatus.FAILED)
                    .failureReason(reason)
                    .build();

            Payment savedPayment = paymentRepository.save(payment);

            PaymentFailedEvent failedEvent = PaymentFailedEvent.builder()
                    .paymentId(savedPayment.getId())
                    .orderId(savedPayment.getOrderId())
                    .userEmail(savedPayment.getUserEmail())
                    .amount(savedPayment.getAmount())
                    .failureReason(reason)
                    .failedAt(LocalDateTime.now())
                    .build();

            saveOutboxEvent(
                    savedPayment.getId().toString(),
                    "PaymentFailedEvent",
                    RabbitMQConfig.PAYMENT_FAILED_ROUTING_KEY,
                    failedEvent
            );

            log.warn("Payment FAILED for orderId={}, paymentId={}, reason={}",
                    savedPayment.getOrderId(), savedPayment.getId(), reason);
        }
    }

    private boolean isPaymentApproved(BigDecimal amount) {
        if (amount == null || amount.compareTo(BigDecimal.ZERO) <= 0) {
            return false;
        }
        return amount.compareTo(MAX_LIMIT) <= 0;
    }

    private void saveOutboxEvent(String aggregateId, String eventType, String routingKey, Object eventPayload) {
        try {
            String jsonPayload = objectMapper.writeValueAsString(eventPayload);

            OutboxEvent outboxEvent = OutboxEvent.builder()
                    .aggregateType("Payment")
                    .aggregateId(aggregateId)
                    .eventType(eventType)
                    .exchangeName(RabbitMQConfig.PAYMENT_EXCHANGE)
                    .routingKey(routingKey)
                    .payload(jsonPayload)
                    .status(OutboxStatus.PENDING)
                    .build();

            outboxEventRepository.save(outboxEvent);
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Failed to serialize outbox event payload for " + eventType, e);
        }
    }
}