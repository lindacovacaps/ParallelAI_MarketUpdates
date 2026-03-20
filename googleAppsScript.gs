// TODO: set up api keys for parallel + gchat url 
//  Run initialSetup() ONCE manually (from the Run menu) to:
//  - Validate your credentials
//  - Create both time-based triggers automatically



// CONFIGURATION

const CONFIG = {

  // --- Parallel AI ---
  processor: "pro",           // "pro" ($0.10/run) or "ultra" ($0.30/run)
                                // Use "pro-fast" or "ultra-fast" for faster turnaround


  // Polling
  taskTimeoutHours: 3,          // If a run_id is older than this, treat it as stuck and clear it
  pollIntervalMinutes: 10,      // Must match your Trigger 2 interval (set during initialSetup)

  // Nightly trigger 
  nightlyHour: 22,              // 24-hour format. 22 = 10:00 PM in your script's timezone
};



// SCRIPT PROPERTY KEYS 

const PROPS = {
  runId:      "PENDING_RUN_ID",
  startedAt:  "TASK_STARTED_AT",
  apiKey:     "PARALLEL_API_KEY",
  webhookUrl: "GCHAT_WEBHOOK_URL",
};


// TRIGGER 1 — Start the nightly research task

function startResearch() {
  const props     = PropertiesService.getScriptProperties();
  const apiKey    = props.getProperty(PROPS.apiKey);
  const webhookUrl = props.getProperty(PROPS.webhookUrl);

  // Guard: ensure credentials exist
  if (!apiKey || !webhookUrl) {
    console.error("Missing credentials. Run initialSetup() first.");
    return;
  }

  // Guard: exit immediately on weekends and US market holidays
  if (isUSMarketHoliday()) return;

  // Guard: don't start a new task if one is already in flight
  const existingRunId = props.getProperty(PROPS.runId);
  if (existingRunId) {
    console.warn("A task is already in progress (run_id: %s). Skipping new task creation.", existingRunId);
    return;
  }

  console.log("Starting Parallel AI Deep Research task...");

  const payload = {
    input:     buildPrompt().trim(),
    processor: CONFIG.processor,
    task_spec: {
      output_schema: {
        type: "text"
      }
    }
  };

  const options = {
    method:      "post",
    contentType: "application/json",
    headers:     { "x-api-key": apiKey },
    payload:     JSON.stringify(payload),
    muteHttpExceptions: true,
  };

  let response;
  try {
    response = UrlFetchApp.fetch("https://api.parallel.ai/v1/tasks/runs", options);
  } catch (e) {
    console.error("Network error when starting task: %s", e.message);
    sendChatMessage(webhookUrl, "*Parallel AI Automation Error*\nFailed to start nightly research task.\nReason: Network error — " + e.message);
    return;
  }

  const code = response.getResponseCode();
  const body = safeParseJson(response.getContentText());

  if (code !== 200 && code !== 201 && code !=202) {
    const errMsg = (body && body.error) ? body.error : response.getContentText();
    console.error("Parallel AI returned HTTP %s: %s", code, errMsg);
    sendChatMessage(webhookUrl, "*Parallel AI Automation Error*\nFailed to start task. HTTP " + code + ": " + errMsg);
    return;
  }

  const runId = body && body.run_id;
  if (!runId) {
    console.error("No run_id in response: %s", JSON.stringify(body));
    sendChatMessage(webhookUrl, "*Parallel AI Automation Error*\nTask created but no run_id was returned.");
    return;
  }

  // Persist run_id and timestamp for the polling trigger
  props.setProperty(PROPS.runId, runId);
  props.setProperty(PROPS.startedAt, new Date().toISOString());

  console.log("Task started. run_id: %s | Polling will check every %s minutes.", runId, CONFIG.pollIntervalMinutes);
}

// weekends + US holidays

function buildPrompt() {
  const now = new Date();
  const nyTz = "America/New_York";

  // prev cal date in NY
  const nyTodayStr = Utilities.formatDate(now, nyTz, "yyyy-MM-dd");
  const nyToday = new Date(nyTodayStr + "T12:00:00");
  const nyYesterday = new Date(nyToday);
  nyYesterday.setDate(nyToday.getDate()-1);

  const dateStr = Utilities.formatDate(nyYesterday, nyTz, "d MMMM yyyy");

  return `Create a report based on ${dateStr} USA close as per below instructions: ' + 'I want to create a morning summary for Asia Pacific Morning time for the overnight US markets, The target segment is professionals in the Fixed Income and Credit Markets. You can use reliable sources like Bloomberg, Reuters, IFR, CNBC, Investing.com. Saxo.com, Federal reserve bank etc. it should have a table at the bottom with data from S&P 500 index, US 10 Year treasury, Gold Spot (XAU/USD), Silver Spot (XAG/USD), OIl Price Spot (Brent Oil Price Spot) with closing price of US markets and change over previous close of the same market. It should start with a summary of market movements, followed by Main headlines on news around politics, important Economic data. Also add any corporate bond information from the day. IMPORTANT: Please note the following while creating the output: 1. Place EXTREMELY HIGH emphasis on accuracy 2. CRITICAL INSTRUCTION: Verify ALL the data in the table as in your previous research reports, the data was WRONG for S&P and Silver. 3. Remove the linkages from the report section and keep them only at the bottom of the output in case I want to cross check. 4. Make sure to follow below given format for report generation: a) Start with a Headline b) Then have an overall Summary, news on US Equities, Foreign Exchange Markets, Bonds, Commodities, Politics and Policy related. Should be around 100-150 words maximum. c) Then have a section on Market movement, other significant market movement on Foreign Exchange rates, Interest rates, Commodities, Credit spreads etc. d) Next section should be on Policy and Politics. This should include all the policy level important changes or any geo political events which were significant e) Next should be on major data releases and their variances from consensus estimates. f) Finally at the end of report include a table with S&P 500 index, US 10 Year treasury, Gold Spot (XAU/USD), Silver Spot (XAG/USD), OIl Price Spot (Brent Oil Price Spot) with closing price of US markets and change over previous close of the same market ( in case the previous calendar date was holiday, please compare with a day immediately before business day g) IMPORTANT POINTS TO KEEP IN MIND: (i) In the various sections in the report DO NOT include references to citations. (ii) Remove the table on specific equity share price movement - just the write-up without the table is sufficient (iii) Remove any recommendations after each section (in other words any "Takeaway") from the report. We want to present a factual situation as opposed to providing any recommendations.`
}

// TRIGGER 2 — Poll for task completion

function pollResearch() {
  const props      = PropertiesService.getScriptProperties();
  const runId      = props.getProperty(PROPS.runId);

  if (!runId) {
    console.log("No pending task. Exiting.");
    return;
  }

  const apiKey     = props.getProperty(PROPS.apiKey);
  const webhookUrl = props.getProperty(PROPS.webhookUrl);
  const startedAt  = props.getProperty(PROPS.startedAt);

  // Timeout check
  if (startedAt) {
    const ageHours = (new Date() - new Date(startedAt)) / (1000 * 60 * 60);
    if (ageHours > CONFIG.taskTimeoutHours) {
      console.warn("Task timed out after %.1f hours. Clearing.", ageHours);
      props.deleteProperty(PROPS.runId);
      props.deleteProperty(PROPS.startedAt);
      sendChatMessage(webhookUrl, "*Parallel AI Automation Warning*\nNightly research task timed out after " + CONFIG.taskTimeoutHours + " hours.\nrun_id: `" + runId + "`");
      return;
    }
  }

  console.log("Polling status for run_id: %s", runId);

  const options = {
    method:  "get",
    headers: { "x-api-key": apiKey },
    muteHttpExceptions: true,
  };

  // Step 1: Check status (non-blocking)
  let statusResponse;
  try {
    statusResponse = UrlFetchApp.fetch("https://api.parallel.ai/v1/tasks/runs/" + runId, options);
  } catch (e) {
    console.error("Network error when checking status: %s", e.message);
    return;
  }

  const statusBody = safeParseJson(statusResponse.getContentText());
  const status = statusBody && statusBody.status;
  console.log("Task status: %s", status);

  if (status === "running" || status === "queued" || status === "pending") {
    console.log("Still in progress. Will check again in %s minutes.", CONFIG.pollIntervalMinutes);
    return;
  }

  if (status === "failed") {
    console.error("Task failed: %s", JSON.stringify(statusBody.error));
    props.deleteProperty(PROPS.runId);
    props.deleteProperty(PROPS.startedAt);
    sendChatMessage(webhookUrl, "*Parallel AI Automation Error*\nTask failed.\nrun_id: `" + runId + "`\nError: " + JSON.stringify(statusBody.error));
    return;
  }

  if (status === "completed") {
    console.log("Task completed! Fetching full result...");

    // Step 2: Fetch result (task is done so this returns immediately)
    let resultResponse;
    try {
      const resultUrl = "https://api.parallel.ai/v1/tasks/runs/" + runId + "/result";
      console.log("Fetching result from:", resultUrl);
      resultResponse = UrlFetchApp.fetch(resultUrl, options);
      console.log("Result HTTP code:", resultResponse.getResponseCode());
      console.log("Result raw response:", resultResponse.getContentText());
    } catch (e) {
      console.error("Failed to fetch result: %s", e.message);
      return;
    }

    const resultBody = safeParseJson(resultResponse.getContentText());

    const reportContent = resultBody.output && resultBody.output.content;
    console.log("Report content length (chars): ", reportContent ? reportContent.length : 0);

    const fullResult = {
      run_id:       resultBody.run_id || runId,
      processor:    resultBody.processor || CONFIG.processor,
      completed_at: resultBody.modified_at || null,
      output:       resultBody.output,
      status:       "completed"
    };

    const message = formatResearchOutput(fullResult);
    sendChatMessage(webhookUrl, message);
    props.deleteProperty(PROPS.runId);
    props.deleteProperty(PROPS.startedAt);
    console.log("Report delivered and run_id cleared.");
    return;
  }

  console.warn("Unexpected status: '%s'. Will retry next interval.", status);
}


// FORMAT OUTPUT
// Converts the Parallel AI result into a readable Google Chat message
// Adjust this function to match your preferred report layout

function formatResearchOutput(body) {
  const now     = new Date();
  const dateStr = Utilities.formatDate(now, Session.getScriptTimeZone(), "EEEE, MMMM d, yyyy");
  const output  = body.output;

  // Extract metadata — handles both flat and nested run structure
  const run         = body.run || body;
  const runId       = run.run_id      || "unknown";
  const processor   = run.processor   || CONFIG.processor;
  const completedAt = run.modified_at || run.completed_at || null;
  const completedStr = completedAt
    ? Utilities.formatDate(new Date(completedAt), Session.getScriptTimeZone(), "HH:mm:ss z")
    : "unknown";

  let message = "*US Market Pre-Session Brief — " + dateStr + "*\n\n";

  if (output && output.content && typeof output.content === "string") {
    let cleaned = output.content
      // Strip inline citation numbers like [1], [2], [1][2], [1] [2]
      .replace(/(\[\d+\]\s*)+/g, "")

      // Major Headings
      .replace(/^#{1,2}\s+(.+)$/gm, "\n---------------\n*$1*")
      // Minor Headings 
      .replace(/^#{3,6}\s+(.+)$/gm, "\n*$1*")

      // Normalise bold
      .replace(/\*\*(.*?)\*\*/g, "*$1*")

      // Remove table separator rows
      .replace(/\|[-:\s|]+\|\n/g, "")
      // Convert table rows to bullet points
      .replace(/\|(.*?)\|(.*?)\|(.*?)\|/g, function(match, col1, col2, col3) {
        return "• " + col1.trim() + ": " + col2.trim() + " (" + col3.trim() + ")";
      })

      // Collapse excess blank lines
      .replace(/\n{3,}/g, "\n\n")
      .trim();

    message += cleaned;

  } else if (output && output.content && typeof output.content === "object") {
    message += formatStructuredOutput(output.content);
  } else {
    message += "_No structured content returned. Raw output logged to Apps Script console._";
    console.log("Raw output:", JSON.stringify(body));
  }

  message += "\n\n---\n_Processor: " + processor + " | Completed: " + completedStr + " | run\\_id: " + runId + "_";

  return message;
}


// FORMAT STRUCTURED OUTPUT (auto schema)
// Flattens key JSON fields into a readable text block
// Adjust field names to match your actual prompt output

function formatStructuredOutput(content) {
  let text = "";

  // Iterate over top-level keys and render them
  for (const key in content) {
    if (!content.hasOwnProperty(key)) continue;
    const label = key.replace(/_/g, " ").replace(/\b\w/g, c => c.toUpperCase());
    const value = content[key];

    if (typeof value === "string" || typeof value === "number") {
      text += "*" + label + ":* " + value + "\n\n";
    } else if (Array.isArray(value)) {
      text += "*" + label + ":*\n";
      value.forEach(function(item, i) {
        if (typeof item === "string") {
          text += "• " + item + "\n";
        } else if (typeof item === "object") {
          text += "• " + JSON.stringify(item) + "\n";
        }
      });
      text += "\n";
    } else if (typeof value === "object" && value !== null) {
      text += "*" + label + ":* " + JSON.stringify(value) + "\n\n";
    }
  }

  return text || "_Output was empty._";
}


// SEND TO GOOGLE CHAT
// Posts a message to the configured incoming webhook URL

function sendChatMessage(webhookUrl, text) {
  if (!webhookUrl) {
    console.error("No webhook URL configured.");
    return;
  }

  const maxLength = 3800;

  // Split into clean chunks — never duplicate
  const chunks = [];
  let remaining = text;

  while (remaining.length > 0) {
    if (remaining.length <= maxLength) {
      chunks.push(remaining);
      break;
    }

    // Find the last newline before the limit to avoid cutting mid-sentence
    let splitAt = remaining.lastIndexOf("\n", maxLength);
    if (splitAt === -1) splitAt = maxLength; // No newline found, hard cut

    chunks.push(remaining.substring(0, splitAt).trim());
    remaining = remaining.substring(splitAt).trim();
  }

  console.log("Sending report in %s part(s). Total chars: %s", chunks.length, text.length);

  chunks.forEach(function(chunk, index) {
    const label = chunks.length > 1 ? "_Part " + (index + 1) + " of " + chunks.length + "_\n\n" : "";
    const payload = JSON.stringify({ text: label + chunk });

    const options = {
      method:      "post",
      contentType: "application/json",
      payload:     payload,
      muteHttpExceptions: true,
    };

    try {
      const response = UrlFetchApp.fetch(webhookUrl, options);
      const code = response.getResponseCode();
      if (code === 200) {
        console.log("Part %s of %s posted to Google Chat.", index + 1, chunks.length);
      } else {
        console.error("Google Chat returned HTTP %s for part %s: %s", code, index + 1, response.getContentText());
      }
    } catch (e) {
      console.error("Failed to post part %s: %s", index + 1, e.message);
    }

    // Pause between messages to avoid rate limiting
    if (index < chunks.length - 1) {
      Utilities.sleep(1500);
    }
  });
}


// HELPER — Clear pending task state

function clearPendingTask(props) {
  props.deleteProperty(PROPS.runId);
  props.deleteProperty(PROPS.startedAt);
}


// HELPER — Safe JSON parse

function safeParseJson(text) {
  try {
    return JSON.parse(text);
  } catch (e) {
    return null;
  }
}


// INITIAL SETUP — Run this ONCE manually to create both triggers

function initialSetup() {
  const props      = PropertiesService.getScriptProperties();
  const apiKey     = props.getProperty(PROPS.apiKey);
  const webhookUrl = props.getProperty(PROPS.webhookUrl);

  // Validate credentials exist
  if (!apiKey) {
    throw new Error("PARALLEL_API_KEY not found in Script Properties. Add it before running setup.");
  }
  if (!webhookUrl) {
    throw new Error("GCHAT_WEBHOOK_URL not found in Script Properties. Add it before running setup.");
  }

  // Remove any existing triggers to avoid duplicates
  const existing = ScriptApp.getProjectTriggers();
  existing.forEach(t => ScriptApp.deleteTrigger(t));
  console.log("Removed %s existing trigger(s).", existing.length);

  // Trigger 1: Nightly research kick-off
  ScriptApp.newTrigger("startResearch")
    .timeBased()
    .everyDays(1)
    .atHour(CONFIG.nightlyHour)
    .create();
  console.log("Trigger 1 created: startResearch() at %s:00 daily.", CONFIG.nightlyHour);

  // Trigger 2: 10-minute polling
  ScriptApp.newTrigger("pollResearch")
    .timeBased()
    .everyMinutes(10)
    .create();
  console.log("Trigger 2 created: pollResearch() every 10 minutes.");

  // Send a test message to confirm the Chat webhook is working
  sendChatMessage(webhookUrl,
    "*Parallel AI Research Automation — Setup Complete*\n\n" +
    "Your nightly market brief automation is now active.\n" +
    "• Research starts nightly at *" + CONFIG.nightlyHour + ":00*\n" +
    "• Results polled every *10 minutes*\n" +
    "• Processor: *" + CONFIG.processor + "*\n\n" +
    "_First report will arrive tomorrow morning._"
  );

  console.log("Setup complete. A confirmation message has been sent to Google Chat.");
}


// MANUAL UTILITIES — Run these from the Apps Script editor as needed

// Force-start a research task right now (for testing)
function manualStartResearch() {
  console.log("Manually starting research task...");
  startResearch();
}

// Check the status of the current pending task
function checkStatus() {
  const props  = PropertiesService.getScriptProperties();
  const runId  = props.getProperty(PROPS.runId);
  const start  = props.getProperty(PROPS.startedAt);

  if (!runId) {
    console.log("No task currently in progress.");
    return;
  }

  const ageMs    = start ? (new Date() - new Date(start)) : null;
  const ageMin   = ageMs ? Math.round(ageMs / 60000) : "unknown";
  console.log("Pending run_id: %s | Started: %s | Age: %s minutes", runId, start, ageMin);
}

// Manually clear a stuck run_id (use if a task gets stuck)
function forceClearPendingTask() {
  const props = PropertiesService.getScriptProperties();
  const runId = props.getProperty(PROPS.runId);
  if (!runId) {
    console.log("Nothing to clear.");
    return;
  }
  clearPendingTask(props);
  console.log("Cleared pending run_id: %s", runId);
}

// List all active triggers (for debugging)
function listTriggers() {
  const triggers = ScriptApp.getProjectTriggers();
  if (triggers.length === 0) {
    console.log("No triggers found. Run initialSetup() to create them.");
    return;
  }
  triggers.forEach(function(t) {
    console.log("Trigger: %s | Handler: %s | Type: %s",
      t.getUniqueId(),
      t.getHandlerFunction(),
      t.getTriggerSource()
    );
  });
}

function tempClear() {
  PropertiesService.getScriptProperties().deleteProperty("PENDING_RUN_ID");
  PropertiesService.getScriptProperties().deleteProperty("TASK_STARTED_AT");
  console.log("Cleared.");
}

function isUSMarketHoliday() {
  const now = new Date();

  // All US market date logic must reference New York time, not Singapore time.
  // At 10 PM SGT, it is ~9 AM ET the same calendar day — so the last US
  // market close was the previous calendar day in New York.
  const nyTz = "America/New_York";

  // Get yesterday's date in New York time
  const nyTodayStr    = Utilities.formatDate(now, nyTz, "yyyy-MM-dd");
  const nyToday       = new Date(nyTodayStr + "T12:00:00");
  const nyYesterday   = new Date(nyToday);
  nyYesterday.setDate(nyToday.getDate() - 1);

  const day       = parseInt(Utilities.formatDate(nyYesterday, nyTz, "u")); // 1=Mon, 7=Sun
  const monthDay  = Utilities.formatDate(nyYesterday, nyTz, "MM-dd");
  const year      = parseInt(Utilities.formatDate(nyYesterday, nyTz, "yyyy"));
  const dateStr   = Utilities.formatDate(nyYesterday, nyTz, "yyyy-MM-dd");

  // Skip weekends
  if (day === 6 || day === 7) {
    console.log("Last NY trading day was a weekend (%s). Skipping.", dateStr);
    return true;
  }

  // Fixed holidays
  const fixedHolidays = ["01-01", "06-19", "07-04", "12-25"];
  if (fixedHolidays.indexOf(monthDay) !== -1) {
    console.log("Last NY trading day was a fixed US holiday (%s). Skipping.", monthDay);
    return true;
  }

  // Floating holidays
  const floatingHolidays = getFloatingHolidays(year);
  if (floatingHolidays.indexOf(dateStr) !== -1) {
    console.log("Last NY trading day was a floating US holiday (%s). Skipping.", dateStr);
    return true;
  }

  return false;
}


function getFloatingHolidays(year) {
  const holidays = [];

  // MLK Day — 3rd Monday of January
  holidays.push(getNthWeekdayOfMonth(year, 1, 1, 3));

  // Presidents' Day — 3rd Monday of February
  holidays.push(getNthWeekdayOfMonth(year, 2, 1, 3));

  // Good Friday — 2 days before Easter (NYSE closes)
  const easter    = getEasterDate(year);
  const goodFriday = new Date(easter);
  goodFriday.setDate(easter.getDate() - 2);
  holidays.push(Utilities.formatDate(goodFriday, Session.getScriptTimeZone(), "yyyy-MM-dd"));

  // Memorial Day — last Monday of May
  holidays.push(getLastWeekdayOfMonth(year, 5, 1));

  // Labor Day — 1st Monday of September
  holidays.push(getNthWeekdayOfMonth(year, 9, 1, 1));

  // Thanksgiving — 4th Thursday of November
  holidays.push(getNthWeekdayOfMonth(year, 11, 4, 4));

  // Handle fixed holidays that fall on weekend — observe Friday or Monday
  const fixed = [
    new Date(year, 0, 1),  // New Year's Day
    new Date(year, 5, 19), // Juneteenth
    new Date(year, 6, 4),  // Independence Day
    new Date(year, 11, 25) // Christmas
  ];

  fixed.forEach(function(date) {
    const dow = date.getDay(); // 0=Sun, 6=Sat
    if (dow === 6) {
      // Saturday → observed Friday
      const observed = new Date(date);
      observed.setDate(date.getDate() - 1);
      holidays.push(Utilities.formatDate(observed, Session.getScriptTimeZone(), "yyyy-MM-dd"));
    } else if (dow === 0) {
      // Sunday → observed Monday
      const observed = new Date(date);
      observed.setDate(date.getDate() + 1);
      holidays.push(Utilities.formatDate(observed, Session.getScriptTimeZone(), "yyyy-MM-dd"));
    }
  });

  return holidays;
}


// Returns "yyyy-MM-dd" string for the Nth occurrence of a weekday in a month
// weekday: 0=Sun, 1=Mon, 2=Tue, 3=Wed, 4=Thu, 5=Fri, 6=Sat
function getNthWeekdayOfMonth(year, month, weekday, n) {
  const date  = new Date(year, month - 1, 1);
  let count   = 0;
  while (true) {
    if (date.getDay() === weekday) {
      count++;
      if (count === n) break;
    }
    date.setDate(date.getDate() + 1);
  }
  return Utilities.formatDate(date, Session.getScriptTimeZone(), "yyyy-MM-dd");
}


// Returns "yyyy-MM-dd" string for the last occurrence of a weekday in a month
function getLastWeekdayOfMonth(year, month, weekday) {
  const date = new Date(year, month, 0); // Last day of month
  while (date.getDay() !== weekday) {
    date.setDate(date.getDate() - 1);
  }
  return Utilities.formatDate(date, Session.getScriptTimeZone(), "yyyy-MM-dd");
}


// Anonymous Gregorian algorithm for Easter date
function getEasterDate(year) {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day   = ((h + l - 7 * m + 114) % 31) + 1;
  return new Date(year, month - 1, day);
}
