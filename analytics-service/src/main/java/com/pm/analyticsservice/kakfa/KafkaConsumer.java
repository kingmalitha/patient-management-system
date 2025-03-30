package com.pm.analyticsservice.kakfa;

import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import patient.events.PatientEvent;

@Service
public class KafkaConsumer {

    private static final Logger log = LoggerFactory.getLogger(KafkaConsumer.class);
    @KafkaListener(topics = "patient", groupId = "analytics-service")
    public void consumeEvent(byte[] event) {
        try {
            PatientEvent patientEvent = PatientEvent.parseFrom(event);
            // PERFORM ANY BUSINESS LOGIC HERE

            log.info("Received Patient Event: [Patient ID={}, PatientName={}," +
                    " PatientEmail={},Event Type={}]",
                    patientEvent.getPatientId(),
                    patientEvent.getName(),
                    patientEvent.getEmail(),
                    patientEvent.getEventType());
        } catch (InvalidProtocolBufferException e) {
            log.error("Error deserialize event {}", e.getMessage());
        }
    }
}
