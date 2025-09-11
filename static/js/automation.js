let isConversionRunning = false;
let isSocialMediaRunning = false;
let isBusinessProcessRunning = false;
let isNsuFtdTrackerRunning = false;
let isBadshaReportRunning = false;

const conversionBtn = document.getElementById("conversionBtn");
const mediaBtn = document.getElementById("mediaBtn");
const businessBtn = document.getElementById("businessBtn");
const nsuftdBtn = document.getElementById("nsuftdBtn");
const badshaBtn = document.getElementById("badshaBtn")
const logPanel = document.getElementById("job-log-panel");
const logContent = document.getElementById("job-log-content");

// Utility: Poll logs from backend
const pollLogs = (jobId, jobType) => {
if (!jobId || !jobType) return;

const logPanel = document.getElementById("job-log-panel");
const logContent = document.getElementById("job-log-content");

logPanel.classList.remove("hidden");
logContent.textContent = "[INFO] Starting log stream...\n";

const interval = setInterval(async () => {
    try {
    const res = await fetch(`/api/v1/${jobType}/logs/${jobId}`);
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
        localStorage.removeItem("businessProcessRunning");
        localStorage.removeItem("businessProcessJobId");
        localStorage.removeItem("nsuFtdTrackerRunning");
        localStorage.removeItem("nsuFtdTrackerJobId");
        localStorage.removeItem("badshaReportRunning");
        localStorage.removeItem("badshaReportJobId");

        isConversionRunning = false;
        isSocialMediaRunning = false;
        isBusinessProcessRunning = false;
        isNsuFtdTrackerRunning = false;
        isBadshaReportRunning = false;

        conversionBtn.disabled = false;
        conversionBtn.textContent = "Start Conversion";
        
        mediaBtn.disabled = false;
        mediaBtn.textContent = "Start Social";

        businessBtn.disabled = false;
        businessBtn.textContent = "Start Process";

        nsuftdBtn.disabled = false;
        nsuftdBtn.textContent = "Start Tracker";

        badshaBtn.disabled = false;
        badshaBtn.textContent = "Start Badsha"
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
            pollLogs(data.job_id, "conversion");
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
            pollLogs(data.job_id, "media");
        }

        console.log("Social media automation started.");
        localStorage.removeItem("socialMediaRunning");
    } catch (err) {
        console.error("Social media error:", err);
        console.error("Social media automation failed.");
        localStorage.removeItem("socialMediaRunning");
    }
};

// Business Process Button Action
const startBusinessProcessAutomation = async (formData) => {
    if (isBusinessProcessRunning || localStorage.getItem("businessProcessRunning") === "true") {
        console.log("Business Process is already running.")
        return;
    }
    const {
        selectedBrand,
        selectedCurrency,
        selectedTimeGrain,
        startDate,
        endDate
    } = formData;

    isBusinessProcessRunning = true;
    localStorage.setItem("businessProcessRunning", "true");

    businessBtn.disabled = true;
    businessBtn.textContent = "Processing...";

    try {
        const res = await fetch("/api/v1/business/start", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                brand: selectedBrand,
                currency: selectedCurrency,
                timeGrain: selectedTimeGrain,
                startDate,
                endDate
            })
        });
        const data = await res.json();
        console.log("Business Process Done: ", data);
        if (data.job_id) {
            localStorage.setItem("businessProcessJobId", data.job_id);
            pollLogs(data.job_id, "business");
        }

        console.log("Business Process Started Succesfully..");
        localStorage.removeItem("businessProcessRunning")
    } catch (err) {
        console.error("Business Process Error: ", err);
        console.log("Business Process Failed,");
        localStorage.removeItem("businessProcessRunning")

    }
};

// NSU/FTD Tracker Button Action
const StartNsuFtdTrackerAutomation = async () => {
    if (isNsuFtdTrackerRunning || localStorage.getItem("nsuFtdTrackerRunning") === "true") {
        console.log("NSU/FTD Tracker Process is Already Running...")
        return;
    }

    isNsuFtdTrackerRunning = true;
    localStorage.setItem("nsuFtdTrackerRunning", "true");

    nsuftdBtn.disabled = true;
    nsuftdBtn.textContent = "Processing...";

    try {
        const res = await fetch("/api/v1/tracker/start", {
            method: "POST"
        });
        const data = await res.json();
        console.log("NSU/FTD Tracker Done: ", data);

        if(data.job_id) {
            localStorage.setItem("nsuFtdTrackerJobId", data.job_id);
            pollLogs(data.job_id, "tracker")
        }
        console.log("NSU/FTD Tracker started successfully.");
        localStorage.removeItem("nsuFtdTrackerRunning");
    } catch (err) {
        console.error("NSU/FTD Tracker Error: ", err);
        console.error("NSu/FTD Tracker Automation Failed.");
        localStorage.removeItem("nsuFtdTrackerRunning")
    }
};

// Badsha Report Automation / BO FTD NSU
const BadshaReportAutomation = async (formData) => {
    if (isBadshaReportRunning || localStorage.getItem("badshaReportRunning") === "true") {
        console.log("Badsha Report Process is Already Running...")
        return;
    }
    const {
        startDate
    } = formData;
    isBadshaReportRunning = true;
    localStorage.setItem("badshaReportRunning", "true");

    badshaBtn.disabled = true;
    badshaBtn.textContent = "Processing...";

    try {
        const res = await fetch("/api/v1/badsha/start", {
            method: "POST",
            headers: {
                "Content-Type": "application/json"
            },
            body: JSON.stringify({
                startDate
            })
        });
        const data = await res.json();
        console.log("Badsha Report Done: ", data);
        
        if (data.job_id) {
            localStorage.setItem("badshaReportJobId", data.job_id);
            pollLogs(data.job_id, "badsha")
        }
        console.log("Badsha started successfully.");
        localStorage.removeItem("badshaReportRunning");
    } catch (err) {
        console.error("Badsha Report Error: ", err);
        console.error("Badsha Report Automation Failed.");
        localStorage.removeItem("badshaReportRunning")
    }
};



// On page load
window.addEventListener("DOMContentLoaded", () => {
    const conversionRunning = localStorage.getItem("conversionRunning") === "true";
    const socialMediaRunning = localStorage.getItem("socialMediaRunning") === "true";
    const businessProcessRunning = localStorage.getItem("businessProcessRunning") === "true";
    const nsuFtdTrackerRunning = localStorage.getItem("nsuFtdTrackerRunning") === "true";
    const badshaReportRunning = localStorage.getItem("badshaReportRunning") === "true";

    if (conversionRunning) {
        conversionBtn.disabled = true;
        conversionBtn.textContent = "Processing...";
        isConversionRunning = true;

        const jobId = localStorage.getItem("conversionJobId");
        if (jobId) pollLogs(jobId, "conversion");
    }

    if (socialMediaRunning) {
        mediaBtn.disabled = true;
        mediaBtn.textContent = "Processing...";
        isSocialMediaRunning = true;

        const jobId = localStorage.getItem("socialMediaJobId");
        if (jobId) pollLogs(jobId, "media");
        
    }

    if (businessProcessRunning) {
        businessBtn.disabled = true;
        businessBtn.textContent = "Processing...";
        isBusinessProcessRunning = true;

        const jobId = localStorage.getItem("businessProcessJobId");
        if (jobId) pollLogs(jobId, "business");
    }

    if (nsuFtdTrackerRunning) {
        nsuftdBtn.disabled = true;
        nsuftdBtn.textContent = "Processing...";
        isNsuFtdTrackerRunning = true;

        const jobId = localStorage.getItem("nsuFtdTrackerJobId");
        if (jobId) pollLogs(jobId, "tracker");
    }

    if (badshaReportRunning) {
        badshaBtn.disabled = true;
        badshaBtn.textContent = "Processing...";
        isBadshaReportRunning = true;

        const jobId = localStorage.getItem("badshaReportJobId");
        if (jobId) pollLogs(jobId, "badsha")
    }

});
