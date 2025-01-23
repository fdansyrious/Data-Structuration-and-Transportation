from typing import Annotated
from fastapi import APIRouter, HTTPException, status, Response, Depends,  Query, Path

from config.env import get_session
from db.models import Teachers
from sqlmodel import Session, select
        
SessionDep = Annotated[Session, Depends(get_session)]

router = APIRouter(
    prefix="/teachers",
    tags=["teachers"],
    responses={status.HTTP_201_CREATED: {"desciption": "Requested creation done"},
               status.HTTP_404_NOT_FOUND: {"description": "Requested element not found"},
               status.HTTP_500_INTERNAL_SERVER_ERROR: {"description": "Internal server error"}},
)

# ------ CRUD operations definition ------

# ------ Create operation
@router.post("/", response_model=Teachers, summary="Insert a teacher with the provided details in the teacher data table")
def create_teacher(teacher: Teachers, 
                   session: SessionDep) :
    try:
        session.add(teacher)
        session.commit()
        session.refresh(teacher)
        return teacher
    except Exception as e:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Exception raised {e}")



#  ------ Read operations

# Retrieving informations of the collections
@router.get("/", response_model=dict
            , summary="List teachers information from offset+1 to the desired limit"
            )
def read_teachers(session: SessionDep, 
                        response: Response,
                        offset: Annotated[int, Query(description="number of records to skip")]=0, 
                        limit: Annotated[int, Query(le=10, description="maximum number of records to retrieve")]=5,
                        ) :
    query = select(Teachers).offset(offset=offset).limit(limit=limit)
    results = session.exec(query)
    teachers = results.all()
    
    return {"data" : teachers, "count": len(teachers)}

# Retrieving informations of a single teacher
@router.get("/{teacher_id}", response_model=Teachers, summary="List a specific teacher's informations using his id")
def read_teacher_from_id(session: SessionDep, 
                         teacher_id: Annotated[str, Path(description="Id of the teacher to retrieve data for")],
                         response: Response) :
    response.headers["Cache-Control"] = "no-cache"
    
    query = select(Teachers).where(Teachers.TeacherID==teacher_id)
    results = session.exec(query)
    teacher = results.fetchall()
    print(teacher)
    if not teacher:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"Teacher with id: {teacher_id} not found")
    teacher_dict = teacher[0].model_dump()
    return teacher_dict

# ------ Update operations

# Full upadte operations
@router.put("/{teacher_id}", response_model=Teachers,
                         status_code=status.HTTP_200_OK)
def update_teacher(session: SessionDep, 
                   teacher_id: Annotated[str, Path(description="Id of the teacher whose informations to update")],
                   teacher: Teachers):
    teacher_db_info = session.get(Teachers, teacher_id) #equivalent to l56 -> l58
    if not teacher_db_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"teacher with id: {teacher_id} not found")
    teacher_new_data = teacher.model_dump()
    teacher_db_info.sqlmodel_update(teacher_new_data)
    session.add(teacher_db_info)
    session.commit()
    session.refresh(teacher_db_info)
    return teacher_db_info

# Partial upadte operations
@router.patch("/{teacher_id}", response_model=Teachers,
                         status_code=status.HTTP_200_OK)
def partially_update_teacher(session: SessionDep, 
                   teacher_id: Annotated[str, Path(description="Id of the teacher whose informations to update")],
                   teacher: Teachers):
    teacher_db_info = session.get(Teachers, teacher_id)
    if not teacher_db_info:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"teacher with id: {teacher_id} not found")
    teacher_new_data = teacher.model_dump(exclude_unset=True)
    teacher_db_info.sqlmodel_update(teacher_new_data)
    session.add(teacher_db_info)
    session.commit()
    session.refresh(teacher_db_info)
    return teacher_db_info

# ------ Delete operation
@router.delete("/{teacher_id}", summary="Delete specific teacher's informations using his id",
                         status_code=status.HTTP_200_OK)
def delete_teacher(session: SessionDep, 
                teacher_id: Annotated[str, Path(description="Id of the teacher to remove from the table")]):
    teacher = session.get(Teachers, teacher_id)
    if not teacher:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=f"teacher with id: {teacher_id} not found")
    session.delete(teacher)
    session.commit()
    return {status.HTTP_200_OK: f"Teacher {teacher_id} successfully delete"}