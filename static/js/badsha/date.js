document.addEventListener("alpine:init", () => {
    Alpine.data("badshaAutomation", () => {

        const today = new Date();
        const yesterday = new Date(today);
        yesterday.setDate(today.getDate() - 1);

        const formatDate = (date) => date.toISOString().split("T")[0];

        return {
            startDate: formatDate(today),

            sendToAutomation() {
                const payload  = {
                    startDate: this.startDate
                };
                // Call function defined in automation.js
                startJob('badsha', payload);
            }
        }
    });
});