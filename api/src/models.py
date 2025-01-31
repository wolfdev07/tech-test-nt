from pydantic import BaseModel, Field

class NumberInput(BaseModel):
    number_extract: int = Field(..., ge=1, le=100, description="Número entre 1 y 100")
