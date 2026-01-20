package com.thesis.common.exception;

/**
 * Exception thrown when an inventory reservation cannot be found.
 */
public class ReservationNotFoundException extends ResourceNotFoundException {

    public ReservationNotFoundException(String reservationId) {
        super("Reservation", reservationId);
    }

    public ReservationNotFoundException(String reservationId, Throwable cause) {
        super("Reservation", reservationId, cause);
    }
}
