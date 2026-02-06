package com.thesis.common.exception;

public class ReservationNotFoundException extends ResourceNotFoundException {

    public ReservationNotFoundException(String reservationId) {
        super("Reservation", reservationId);
    }

    public ReservationNotFoundException(String reservationId, Throwable cause) {
        super("Reservation", reservationId, cause);
    }
}
