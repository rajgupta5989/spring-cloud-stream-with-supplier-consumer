package com.example.rabbit;

public interface StreamProcessor {

    String TEST_JOB_OUTPUT = "sendMessage-out-0";
    String TEST_JOB_OUTPUT_2 = "sendMessage2-out-0";

   // @Output(StreamProcessor.TEST_JOB_OUTPUT)
   // MessageChannel testJobOutput();
}
