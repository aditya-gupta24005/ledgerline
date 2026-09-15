package dev.ledgerline.order.api;

import dev.ledgerline.order.engine.EngineHaltedException;
import dev.ledgerline.order.engine.InvalidPriceException;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

@RestControllerAdvice
class ApiExceptionHandler {

    @ExceptionHandler(OrderRejectedException.class)
    ProblemDetail handleRejected(OrderRejectedException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatusCode.valueOf(422), e.getMessage());
        problem.setTitle("Order rejected");
        problem.setProperty("orderId", e.orderId());
        return problem;
    }

    @ExceptionHandler(InvalidPriceException.class)
    ProblemDetail handleInvalidPrice(InvalidPriceException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.BAD_REQUEST, e.getMessage());
        problem.setTitle("Invalid price");
        return problem;
    }

    @ExceptionHandler(EngineHaltedException.class)
    ProblemDetail handleHalted(EngineHaltedException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(HttpStatus.SERVICE_UNAVAILABLE, e.getMessage());
        problem.setTitle("Engine halted");
        return problem;
    }
}
