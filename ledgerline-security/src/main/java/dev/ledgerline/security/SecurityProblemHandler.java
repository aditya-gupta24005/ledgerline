package dev.ledgerline.security;

import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.security.access.AccessDeniedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/** Method-security denials reach the controller layer as {@link AccessDeniedException}; answer with a 403 problem. */
@RestControllerAdvice
public class SecurityProblemHandler {

    @ExceptionHandler(AccessDeniedException.class)
    ProblemDetail handleAccessDenied(AccessDeniedException e) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(
                HttpStatus.FORBIDDEN, "You do not have permission to perform this action");
        problem.setTitle("Forbidden");
        return problem;
    }
}
