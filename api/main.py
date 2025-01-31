from fastapi import FastAPI, HTTPException
from src.models import NumberInput
from src.services import NumberSet

app = FastAPI()
number_set = NumberSet()

@app.post("/extract")
def extract_number(input_data: NumberInput):
    try:
        number_set.extract(input_data.number_extract)
        return {"message": f"Número {input_data.number_extract} eliminado exitosamente"}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

@app.get("/find_missing")
def find_missing():
    try:
        return {"missing_numbers": number_set.find_missing()}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))