document.addEventListener("alpine:init", () => {
    Alpine.data("winbdtProcessAutomation", () => {
        const today = new Date();
        const yesterday = new Date(today);
        yesterday.setDate(today.getDate() - 1);

        const formatDate = (date) => date.toISOString().split("T")[0];

        return {
            
            startDate: formatDate(yesterday),
            endDate: formatDate(yesterday),
            selectedTimeGrain: "",

            sendToAutomation() {
                const payload  = {
                    startDate: this.startDate,
                    endDate: this.endDate,
                    timeGrain: this.selectedTimeGrain
                };

                // Call function defined in automation.js
                startWinBdtProcessAutomation(payload);
            }
        }
    });
});