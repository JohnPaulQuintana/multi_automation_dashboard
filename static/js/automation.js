let isConversionRunning = false;
let isSocialMediaRunning = false;

const conversionBtn = document.getElementById("conversionBtn");
const mediaBtn = document.getElementById("mediaBtn");
const logPanel = document.getElementById("job-log-panel");
const logContent = document.getElementById("job-log-content");

// Utility: Poll logs from backend
const pollLogs = (jobId) => {
  if (!jobId) return;

  const logPanel = document.getElementById("job-log-panel");
  const logContent = document.getElementById("job-log-content");

  logPanel.classList.remove("hidden");
  logContent.textContent = "[INFO] Starting log stream...\n";

  const interval = setInterval(async () => {
    try {
      const res = await fetch(`/api/v1/conversion/logs/${jobId}`);
      if (!res.ok) throw new Error("Log fetch failed");

      const data = await res.json();
      const logs = data.logs || [];

      const now = new Date().toLocaleString();

      // Map logs with inline fallback for null entries
      const renderedLogs = logs.map(line => {
        if (line === null || line === undefined) {
          return `[${now}] ⏳ Still processing...`;
        }
        return `[${now}] ${line}`;
      });

      logContent.textContent = renderedLogs.join("\n");
      logPanel.scrollTop = logPanel.scrollHeight;

      // Detect completion
      if (logs.at(-1)?.includes("✅ Job complete")) {
        clearInterval(interval);
        localStorage.removeItem("conversionRunning");
        localStorage.removeItem("conversionJobId");
        localStorage.removeItem("socialMediaRunning");
        localStorage.removeItem("socialMediaJobId");

        isConversionRunning = false;
        isSocialMediaRunning = false;

        conversionBtn.disabled = false;
        conversionBtn.textContent = "Start Conversion";
        mediaBtn.disabled = false;
        mediaBtn.textContent = "Start Social";
      }

    } catch (err) {
      console.error("Log polling error:", err);
      clearInterval(interval);
    }
  }, 2000);
};


const copyLogs = () => {
  const content = document.getElementById("job-log-content").textContent;
  navigator.clipboard.writeText(content).then(() => {
    alert("Logs copied to clipboard!");
  });
};



// Conversion button action
const startConversionAutomation = async () => {
    if (isConversionRunning || localStorage.getItem("conversionRunning") === "true") {
        console.log("Conversion process is already running.");
        return;
    }

    isConversionRunning = true;
    localStorage.setItem("conversionRunning", "true");

    conversionBtn.disabled = true;
    conversionBtn.textContent = "Processing...";

    try {
        const res = await fetch("/api/v1/conversion/start", { method: "POST" });
        const data = await res.json();
        console.log("Conversion done:", data);

        // Save job ID and start polling
        if (data.job_id) {
            localStorage.setItem("conversionJobId", data.job_id);
            pollLogs(data.job_id);
        }

        // alert is moved after polling starts
        console.log("Conversion started successfully.");
        localStorage.removeItem("conversionRunning");
    } catch (err) {
        console.error("Conversion error:", err);
        console.log("Conversion failed.");
        localStorage.removeItem("conversionRunning");
    }
};

// Social media button action
const startSocialMediaAutomation = async () => {
    if (isSocialMediaRunning || localStorage.getItem("socialMediaRunning") === "true") {
        console.log("Social media process is already running.");
        return;
    }

    isSocialMediaRunning = true;
    localStorage.setItem("socialMediaRunning", "true");

    mediaBtn.disabled = true;
    mediaBtn.textContent = "Processing...";

    try {
        const res = await fetch("/api/v1/media/start", { method: "POST" });
        const data = await res.json();
        console.log("Social media done:", data);

        // Save job ID and start polling
        if (data.job_id) {
            localStorage.setItem("socialMediaJobId", data.job_id);
            pollLogs(data.job_id);
        }

        console.log("Social media automation started.");
        localStorage.removeItem("socialMediaRunning");
    } catch (err) {
        console.error("Social media error:", err);
        console.error("Social media automation failed.");
        localStorage.removeItem("socialMediaRunning");
    }
};

// On page load
window.addEventListener("DOMContentLoaded", () => {
    const conversionRunning = localStorage.getItem("conversionRunning") === "true";
    const socialMediaRunning = localStorage.getItem("socialMediaRunning") === "true";

    if (conversionRunning) {
        conversionBtn.disabled = true;
        conversionBtn.textContent = "Processing...";
        isConversionRunning = true;

        const jobId = localStorage.getItem("conversionJobId");
        if (jobId) pollLogs(jobId);
    }

    if (socialMediaRunning) {
        mediaBtn.disabled = true;
        mediaBtn.textContent = "Processing...";
        isSocialMediaRunning = true;

        const jobId = localStorage.getItem("socialMediaJobId");
        if (jobId) pollLogs(jobId);
    }
});
