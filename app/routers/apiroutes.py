from fastapi import APIRouter, HTTPException

# "/"
# "/sales"
router = APIRouter(
    prefix="/sales",
    tags=["sales"],
    responses={404: {"description": "Not found"}}
)

# localhost:8000/sales/
@router.get("/")
def get_sales_root():
    return {"message": "Hello from sales routes"}

@router.get("/exception")
def get_exception():
    raise HTTPException(status_code=404, detail="Not found")