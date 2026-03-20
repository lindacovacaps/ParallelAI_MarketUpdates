# ParallelAI_MarketUpdates
End-to-end market intelligence system designed as a serverless pipeline using Google Apps Script, integrating external AI services (Parallel AI) with internal communication tools (Google Chat). 

Polliding-based design using time-based triggers to initiate research tasks and periodically check for completion, ensuring robustness without requireing persistent infrastructure. 

## More information
Integration of Parallel AI Deep research API with Google Chat webhooks, handing asynchroous task execution and multi-stage data retreval. Involved interpretation of API behaviour, handling HTTP responses for non-blocking tasks and distinguishing status and result endpoints. 
A text-processing layer convers raw AI-generated markdown into a format compatible with Google Chat. This included restructuring content, removing unsupported elements, and splitting outputs to comply with platform limits. Timezone-aware scheduling and trading-day logic ensures reports reflect the correct US market session when delivered in Asia-Pacific morning. The system is currently deployed and used to deliver daily market intelligence. 
