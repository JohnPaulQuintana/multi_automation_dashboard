# app/automations/business/schema.py

from pydantic import BaseModel

class BusinessAutomationInput(BaseModel):
    brand: str
    currency: str
    timeGrain: str
    startDate: str
    endDate: str
