package com.demo.apachebeam.pipeline.triggers;

import lombok.*;

/**
 * The class {@link LogMessage}
 */
@Data
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Setter
@Getter
public class LogMessage
{
    private String logType;
    private String logSeverity;
    private String logPriority;
    private String logDescription;
}