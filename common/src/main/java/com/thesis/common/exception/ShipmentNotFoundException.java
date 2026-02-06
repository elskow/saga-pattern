package com.thesis.common.exception;

public class ShipmentNotFoundException extends ResourceNotFoundException {

    public ShipmentNotFoundException(String shipmentId) {
        super("Shipment", shipmentId);
    }

    public ShipmentNotFoundException(String shipmentId, Throwable cause) {
        super("Shipment", shipmentId, cause);
    }
}
