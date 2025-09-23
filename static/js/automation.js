const jobs = {
    conversion: {
        button: document.getElementById("conversionBtn"),
        endpoint: "/api/v1/conversion/start",
        stateKey: "conversionRunning",
        jobKey: "conversionJobId"
    },
    media: {
        button: document.getElementById("mediaBtn"),
        endpoint: "/api/v1/media/start",
        stateKey: "socialMediaRunning",
        jobKey: "socialMediaJobId"
    },
    mediaBalance: {
        button: document.getElementById("mediaBalanceBtn"),
        endpoint: "/api/v1/mediaBalance/start",
        stateKey: "mediaBalanceRunning",
        jobKey: "mediaBalanceJobId"
    },
    business: {
        button: document.getElementById("businessBtn"),
        endpoint: "/api/v1/business/start",
        stateKey: "businessProcessRunning",
        jobKey: "businessProcessJobId"
    },
    tracker: {
        button: document.getElementById("nsuftdBtn"),
        endpoint: "/api/v1/tracker/start",
        stateKey: "nsuFtdTrackerRunning",
        jobKey: "nsuFtdTrackerJobId"
    },
    badsha: {
        button: document.getElementById("badshaBtn"),
        endpoint: "/api/v1/badsha/start",
        stateKey: "badshaReportRunning",
        jobKey: "badshaReportJobId"
    },
    winbdt: {
        button: document.getElementById("winBDTBtn"),
        endpoint: "/api/v1/winbdt/start",
        stateKey: "winBdtRunning",
        jobKey: "winBdtJobId"
    },
    badshaProcess: {
        button: document.getElementById("badshaProcessBtn"),
        endpoint: "/api/v1/badshaProcess/start",
        stateKey: "badshaProcessRunning",
        jobKey: "badshaProcessJobId"
    },
    sportsradar: {
        button: document.getElementById("sportsradarBtn"),
        endpoint: "/api/v1/sportsradar/start",
        stateKey: "sportsRadarRunning",
        jobKey: "sportsRadarJobId"
    }
};

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

            const renderedLogs = logs.map(line =>
                line ? `[${now}] ${line}` : `[${now}] ⏳ Still processing...`
            );

            logContent.textContent = renderedLogs.join("\n");
            logPanel.scrollTop = logPanel.scrollHeight;

            if (logs.at(-1)?.includes("✅ Job complete")) {
                clearInterval(interval);

                // Reset job states
                Object.values(jobs).forEach(job => {
                    localStorage.removeItem(job.stateKey);
                    localStorage.removeItem(job.jobKey);
                    job.button.disabled = false;
                    job.button.textContent = `Start`;
                });
            }
            else
                Object.values(jobs).forEach(job => {
                    localStorage.removeItem(job.stateKey);
                    localStorage.removeItem(job.jobKey);
                    job.button.disabled = false;
                    job.button.textContent = `Start`;
                });

        } catch (err) {
            console.error("Log polling error:", err);
            clearInterval(interval);
        }
    }, 2000);
};

// =============================
// Utility
// =============================
const capitalize = (str) =>
    str.charAt(0).toUpperCase() + str.slice(1);

// =============================
// Generic Job Starter
// =============================
const startJob = async (jobType, payload = {}) => {
    const job = jobs[jobType];
    if (!job) return console.error(`Unknown job type: ${jobType}`);

    if (localStorage.getItem(job.stateKey) === "true") {
        console.log(`${capitalize(jobType)} process is already running.`);
        return;
    }

    localStorage.setItem(job.stateKey, "true");
    job.button.disabled = true;
    job.button.textContent = "Processing...";

    try {
        const res = await fetch(job.endpoint, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: Object.keys(payload).length ? JSON.stringify(payload) : null
        });

        const data = await res.json();
        console.log(`${capitalize(jobType)} response:`, data);

        if (data.job_id) {
            localStorage.setItem(job.jobKey, data.job_id);
            pollLogs(data.job_id, jobType);
        }

        console.log(`${capitalize(jobType)} started successfully.`);
    } catch (err) {
        console.error(`${capitalize(jobType)} error:`, err);
        localStorage.removeItem(job.stateKey);
        job.button.disabled = false;
        job.button.textContent = `Start ${capitalize(jobType)}`;
    }
};

// =============================
// Restore States on Page Load
// =============================
window.addEventListener("DOMContentLoaded", () => {
    Object.entries(jobs).forEach(([jobType, job]) => {
        const running = localStorage.getItem(job.stateKey) === "true";
        if (running) {
            job.button.disabled = true;
            job.button.textContent = "Processing...";

            const jobId = localStorage.getItem(job.jobKey);
            if (jobId) pollLogs(jobId, jobType);
        }
    });
});