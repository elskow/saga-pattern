package com.thesis.choreography.order.exception;

import com.thesis.common.exception.OrderNotFoundException;
import lombok.extern.slf4j.Slf4j;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ProblemDetail;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.NoSuchElementException;

@RestControllerAdvice
@Slf4j
public class GlobalExceptionHandler {

    @ExceptionHandler(NoSuchElementException.class)
    public ProblemDetail handleNotFound(NoSuchElementException ex) {
        log.warn("Resource not found: {}", ex.getMessage());
        return buildProblem(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ProblemDetail handleBadRequest(IllegalArgumentException ex) {
        log.warn("Bad request: {}", ex.getMessage());
        return buildProblem(HttpStatus.BAD_REQUEST, ex.getMessage());
    }

    @ExceptionHandler(IllegalStateException.class)
    public ProblemDetail handleConflict(IllegalStateException ex) {
        log.warn("Conflict: {}", ex.getMessage());
        return buildProblem(HttpStatus.CONFLICT, ex.getMessage());
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ProblemDetail handleValidation(MethodArgumentNotValidException ex) {
        log.warn("Validation failed: {}", ex.getBindingResult().getFieldErrors());
        ProblemDetail problem = buildProblem(HttpStatus.BAD_REQUEST, "Validation failed");
        ex.getBindingResult().getFieldErrors().forEach(error ->
            problem.setProperty(error.getField(), error.getDefaultMessage())
        );
        return problem;
    }

    @ExceptionHandler(OrderNotFoundException.class)
    public ProblemDetail handleOrderNotFound(OrderNotFoundException ex) {
        log.warn("Order not found: {}", ex.getMessage());
        return buildProblem(HttpStatus.NOT_FOUND, ex.getMessage());
    }

    @ExceptionHandler(DataAccessException.class)
    public ProblemDetail handleDataAccess(DataAccessException ex) {
        log.error("Database error", ex);
        return buildProblem(HttpStatus.INTERNAL_SERVER_ERROR, "Database operation failed");
    }

    @ExceptionHandler(Exception.class)
    public ProblemDetail handleGeneric(Exception ex) {
        log.error("Unexpected error", ex);
        return buildProblem(HttpStatus.INTERNAL_SERVER_ERROR, "An unexpected error occurred");
    }

    private ProblemDetail buildProblem(HttpStatus status, String message) {
        ProblemDetail problem = ProblemDetail.forStatusAndDetail(status, message);
        problem.setTitle(status.getReasonPhrase());
        return problem;
    }
}
