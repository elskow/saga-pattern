package com.thesis.common.command;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ReleaseInventoryCommand {
    @NotBlank(message = "commandType must not be blank")
    private String commandType;
    
    @NotNull(message = "reservationId must not be null")
    private String reservationId;
    
    @NotNull(message = "orderId must not be null")
    private String orderId;
}
