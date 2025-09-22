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
                S6: ["BDT", "INR", "PKR"],
                WINBDT: ["BDT"],
                BADSHA: ["BDT"],
                WINBDT: ["BDT"],
                SPORTSRADAR: ["BDT"],
            },

        
            get currencyOptions() {
                return this.brandCurrencyMap[this.selectedBrand] || [];
            },

            automationMap: {
                BAJI: (payload) => startJob('business', payload),
                S6: (payload) => startJob('business',payload),
                JB: (payload) => startJob('business',payload),
                CTN: (payload) => startJob('business',payload),
                // WINBDT: (payload) => startJob('business',payload),
                BADSHA: (payload) => startJob('badshaProcess',payload),
                WINBDT: (payload) => startJob('winbdt',payload),
                SPORTSRADAR: (payload) => startJob('sportsradar',payload)
            },

            sendToAutomation() {
                const payload  = {
                    brand: this.selectedBrand,
                    currency: this.selectedCurrency,
                    timeGrain: this.selectedTimeGrain,
                    startDate: this.startDate,
                    endDate: this.endDate
                };

                const automationFn = this.automationMap[this.selectedBrand];
                if (automationFn) {
                    automationFn(payload);
                } else {
                    console.warn(`No automation found for brand ${this.selectedBrand}`);
                }
            }
        }
    });
});