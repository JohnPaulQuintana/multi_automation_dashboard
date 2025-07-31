document.addEventListener("alpine:init", () => {
    Alpine.data("businessAutomation", () => {
        const today = new Date().toISOString().split("T")[0];

        return {
            selectedBrand: "",
            selectedCurrency: "",
            selectedTimeGrain: "",
            startDate: today,
            endDate: today,

            brandCurrencyMap: {
                BAJI: ["BDT", "INR", "NPR", "PKR"],
                CTN: ["AUD", "CNY", "HKD", "MYR ", "SGD"],
                JB: ["BDT", "INR", "PKR"],
                S6: ["BDT", "INR", "PKR"]
            },
        
            get currencyOptions() {
                return this.brandCurrencyMap[this.selectedBrand] || [];
            },

            sendToAutomation() {
                const payload  = {
                    brand: this.selectedBrand,
                    currency: this.selectedCurrency,
                    timeGrain: this.selectedTimeGrain,
                    startDate: this.startDate,
                    endDate: this.endDate
                };

                // Call function defined in automation.js
                startBusinessProcessAutomation(payload);
            }
        }
    });
});