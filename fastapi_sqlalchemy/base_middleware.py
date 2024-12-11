import logging
from functools import reduce
from typing import Any, Dict, Generic, List, Optional, Type, TypeVar, Union

from fastapi import HTTPException
from fastapi.encoders import jsonable_encoder
from fastapi_pagination.ext.sqlalchemy import paginate
from pydantic import BaseModel
from sqlalchemy import select, delete, desc, func, Select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import Query
from sqlalchemy.orm import Session
from sqlalchemy.sql.expression import nulls_last
from starlette import status

import fastapi_sqlalchemy.errors as errors
from fastapi_sqlalchemy.exceptions import BaseHTTPException

ModelType = TypeVar("ModelType", bound=BaseModel)
CreateSchemaType = TypeVar("CreateSchemaType", bound=BaseModel)
UpdateSchemaType = TypeVar("UpdateSchemaType", bound=BaseModel)

logger = logging.getLogger(__name__)


class CRUDBase(Generic[ModelType, CreateSchemaType, UpdateSchemaType]):

    def __init__(self, model: Type[ModelType]):
        """
        CRUD object with default methods to Create, Read, Update, Delete (CRUD).
        **Parameters**
        * `model`: A SQLAlchemy model class
        * `schema`: A Pydantic model (schema) class
        """
        self.model = model
        self.model_name = reduce(
            lambda x, y: x + ('_' if y.isupper() else '') + y, self.model.__name__
        )
        self.id = "id"

    def _active_data(self, query):
        return query.filter(
            getattr(self.model, "is_active") == True,
            getattr(self.model, "is_deleted") == False
        )

    _orm_operator_transformer = {
        "neq": lambda value: ("__ne__", value),
        "gt": lambda value: ("__gt__", value),
        "gte": lambda value: ("__ge__", value),
        "in": lambda value: ("in_", value),
        "isnull": lambda value: ("is_", None) if value is True else ("is_not", None),
        "lt": lambda value: ("__lt__", value),
        "lte": lambda value: ("__le__", value),
        "like": lambda value: ("like", value),
        "ilike": lambda value: ("ilike", value),
        # XXX(arthurio): Mysql excludes None values when using `in` or `not in` filters.
        "not": lambda value: ("is_not", value),
        "not_in": lambda value: ("not_in", value),
    }

    def filter_test(self, query: Union[Query, Select], **kwargs):
        search_filters = []
        for field_name, value in kwargs.items():
            operator = "__eq__"
            if "__" in field_name:
                field = field_name.split("__")
                if field[-1] in self._orm_operator_transformer:
                    field_name = "__".join(field[:-1])
                    operator, value = self._orm_operator_transformer[field[-1]](value)
            # else:
            #     operator = "__eq__"
            attribute = None
            components = field_name.split('__')
            attribute_name = components[0]

            if hasattr(self.model, attribute_name):
                attribute = getattr(self.model, attribute_name)

            if attribute:
                if hasattr(attribute.property, 'direction'):
                    try:
                        print(components)
                        model_field = getattr(self.model, field_name)
                        query = query.filter(getattr(model_field, operator)(value))
                        # search_filters.append(
                        #     attribute.has(text(f"{components[-1]} ILIKE :value"))
                        # )
                    except:
                        pass
                        # search_filters.append(
                        #     attribute.any(text(f"{components[-1]} ILIKE :value"))
                        # )
                else:
                    model_field = getattr(self.model, field_name)
                    query = query.filter(getattr(model_field, operator)(value))
        for field_name, value in kwargs.items():
            if hasattr(self.model, field_name) and value:
                if "__" in field_name:
                    field_name, operator = field_name.split("__")
                    operator, value = self._orm_operator_transformer[operator](value)
                else:
                    operator = "__eq__"
                model_field = getattr(self.model, field_name)
                query = query.filter(getattr(model_field, operator)(value))
        return query

    _orm_operator_transformer = {
        "neq": lambda value: ("__ne__", value),
        "gt": lambda value: ("__gt__", value),
        "gte": lambda value: ("__ge__", value),
        "in": lambda value: ("in_", value),
        "isnull": lambda value: ("is_", None) if value is True else ("is_not", None),
        "lt": lambda value: ("__lt__", value),
        "lte": lambda value: ("__le__", value),
        "like": lambda value: ("like", value),
        "ilike": lambda value: ("ilike", value),
        # XXX(arthurio): Mysql excludes None values when using `in` or `not in` filters.
        "not": lambda value: ("is_not", value),
        "not_in": lambda value: ("not_in", value),
    }

    def is_exist(self, db: Session, **kwargs) -> Optional[ModelType]:
        query = select(self.model)
        for key, value in kwargs.items():
            if hasattr(self.model, key):
                if kwargs.get("updated") and key == self.id:
                    query = query.filter(getattr(self.model, key) != value)
                else:
                    query = query.filter(getattr(self.model, key) == value)
        query = db.execute(query)
        exists = query.scalars().first()
        if exists:
            raise BaseHTTPException(
                detail=f"{self.model_name.replace('_', ' ')} already exists",
                status_code=status.HTTP_400_BAD_REQUEST,
                error_code=getattr(errors, f"OBJECT_ALREADY_EXISTS")
            )
        return True

    def get(self, db: Session, id: Any) -> Optional[ModelType]:
        return db.query(self.model).filter(self.model.id == id).first()

    def filter_by(
            self, db: Session, is_reversed=False, raise_exc=True, join_tables: list = [],
            is_outer: bool = False, **kwargs
    ) -> Optional[ModelType]:
        logger.debug(
            f"Reading from %s, kwargs: %s", self.model.__name__, kwargs
        )
        query = self._active_data(select(self.model))
        for join_table in join_tables:
            query = query.join(join_table, isouter=is_outer)
        for key, value in kwargs.items():
            if hasattr(self.model, key):
                if isinstance(value, list):
                    query = query.filter(getattr(self.model, key).in_(value))
                else:
                    query = query.filter(getattr(self.model, key) == value)
            if key == "inner_filter":
                value = [value] if not isinstance(value, list) else value
                query = query.filter(*value)
        order_by, direction = getattr(self.model, "id"), "desc"
        if kwargs.get("order_by"):
            order_by, direction = kwargs.get("order_by"), kwargs.get("direction")
        if is_reversed:
            query = query.order_by(
                nulls_last(desc(order_by) if direction == "desc" else order_by)
            )
        query = db.execute(query)
        result = query.scalars().first()
        if not result:
            if raise_exc:
                raise BaseHTTPException(
                    detail=f"{self.model_name.replace('_', ' ')} not found",
                    status_code=status.HTTP_404_NOT_FOUND,
                    error_code=getattr(errors, f"NOT_FOUND")
                )
        return result

    def get_multi(
            self,
            db: Session,
            *,
            filter_data=None,
            sorting: bool = True,
            filters: bool = True,
            pagination: bool = True,
            join_tables: list = [],
            is_outer: bool = False,
            **kwargs
    ) -> List[ModelType]:
        logger.debug(f"Reading data from {self.model.__name__}")
        query = self._active_data(select(self.model))
        for join_table in join_tables:
            query = query.join(join_table, isouter=is_outer)
        for key, value in kwargs.items():
            if hasattr(self.model, key) and value:
                if isinstance(value, list):
                    query = query.filter(getattr(self.model, key).in_(value))
                else:
                    query = query.filter(getattr(self.model, key) == value)
            if key == "inner_filter":
                value = [value] if not isinstance(value, list) else value
                query = query.filter(*value)
        if filters:
            query = filter_data.filter(query)
        if sorting:
            query = filter_data.sort(query)
        elif kwargs.get("order_by"):
            order_by = kwargs.get("order_by")
            query = query.order_by(
                nulls_last(desc(order_by) if kwargs.get("direction") == "desc" else order_by)
            )
        else:
            query = query.order_by(desc(getattr(self.model, "id")))
        logger.debug('Query string: %s', query)
        if pagination:
            results = paginate(db, query)
        else:
            query = db.execute(query)
            results = query.unique().scalars().all()
        return results
        # return db.query(self.model).offset(skip).limit(limit).all()

    def create(self, db: Session, *, obj_in: CreateSchemaType,
            autocommit: bool = True,) -> ModelType:
        logger.info(f'Creating {self.model.__name__} with kwargs: {obj_in}')
        try:
            obj_in = jsonable_encoder(obj_in)
            db_obj = self.model(**obj_in)
            db.add(db_obj)
            if autocommit:
                db.commit()
                db.refresh(db_obj)
            else:
                db.flush()
        except IntegrityError as ex:
            db.rollback()
            logger.exception(ex.orig.args)
            raise HTTPException(
                detail=str(ex.orig),
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        logger.info(f'{self.model.__name__} created, data: {db_obj.__dict__}')
        return db_obj

    def update(
            self,
            db: Session,
            *,
            db_obj: ModelType,
            obj_in: Union[UpdateSchemaType, Dict[str, Any]],
            autocommit: bool = True
    ) -> ModelType:

        obj_data = jsonable_encoder(db_obj)
        logger.debug(f"Updating {self.model.__name__} with '{obj_in}' data")
        try:
            if isinstance(obj_in, dict):
                update_data = obj_in
            else:
                update_data = obj_in.dict(exclude_unset=True)
            for field in obj_data:
                if field in update_data:
                    setattr(db_obj, field, update_data[field])
            db.add(db_obj)
            if autocommit:
                db.commit()
                db.refresh(db_obj)
            else:
                db.flush()
        except IntegrityError as ex:
            db.rollback()
            logger.exception(ex.orig.args)
            raise HTTPException(
                detail=str(ex.orig),
                status_code=status.HTTP_400_BAD_REQUEST,
            )
        logger.info(f'{self.model.__name__} updated, data: {db_obj.__dict__}')
        return db_obj

    def remove(self, db: Session, *, id: str) -> ModelType:
        logger.debug(f"Deleting from {self.model.__name__}, id: '{id}'")
        obj = db.query(self.model).with_deleted(db).get(id)
        db.delete(obj)
        db.commit()
        return obj

    def hard_delete(
            self, db: Session, *, id: int, autocommit: bool = True
    ) -> ModelType:
        logger.debug(f"Deleting from {self.model.__name__}, id: '{id}'")
        query = delete(self.model).filter(getattr(self.model, "id") == id)
        db.execute(query)
        db.commit()
        logger.info(f'{self.model.__name__} deleted, id: {id}')
        return True

    def remove_multi(
            self, db: Session, *, autocommit: bool = True, **kwargs
    ) -> ModelType:
        logger.debug(f"Deleting from {self.model.__name__}, kwargs: {kwargs}")
        query = delete(self.model)
        for key, value in kwargs.items():
            if hasattr(self.model, key):
                query = query.filter(getattr(self.model, key) == value)
        db.execute(query)
        db.commit() if autocommit else db.flush()
        logger.info(f'{self.model.__name__} deleted')
        return True

    def count(self, db, **kwargs):
        query = self._active_data(select(func.count(self.model.id)))
        for key, value in kwargs.items():
            if hasattr(self.model, key):
                if isinstance(value, list):
                    query = query.filter(getattr(self.model, key).in_(value))
                else:
                    query = query.filter(getattr(self.model, key) == value)
            if key == "inner_filter":
                query = query.filter(*value)
        count = (db.execute(query)).unique().scalars().first()
        return count
