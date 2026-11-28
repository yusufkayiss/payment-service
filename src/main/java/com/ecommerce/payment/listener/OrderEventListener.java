package com.ecommerce.payment.listener;

import com.ecommerce.payment.config.RabbitMQConfig;
import com.ecommerce.payment.dto.event.OrderCreatedEvent;
import com.ecommerce.payment.service.PaymentService;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.stereotype.Component;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderEventListener {

    private final PaymentService paymentService;

    @RabbitListener(queues = RabbitMQConfig.ORDER_CREATED_QUEUE)
    public void handleOrderCreated(OrderCreatedEvent event) {
        log.info("Received OrderCreatedEvent from queue '{}': orderId={}, amount={}",
                RabbitMQConfig.ORDER_CREATED_QUEUE, event.getOrderId(), event.getTotalAmount());

        paymentService.processOrderPayment(event);
    }
}