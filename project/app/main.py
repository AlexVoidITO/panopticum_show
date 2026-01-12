from contextlib import asynccontextmanager
from io import BytesIO
from datetime import datetime
from typing import Optional
import os

import pandas as pd
from fastapi import Depends, FastAPI, HTTPException, UploadFile, status, Request
from fastapi.responses import HTMLResponse
from sqlmodel import select
from sqlmodel.ext.asyncio.session import AsyncSession
import time
import json

from app.converters import extract_home_data, normalize_dict_to_list
from app.db import get_session, init_db
from app.mathan import analyze_points
from app.models import Point, PointCreate, PointUpdate, PointGet
from app.clickhouse_client import clickhouse_client

import logging

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)

logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Обработчик  цикла работы приложения"""
    # Startup
    await init_db()
    await clickhouse_client.connect()
    logger.info("Application started with ClickHouse logging")
    yield
    # Shutdown
    await clickhouse_client.disconnect()
    logger.info("Application shutdown")


app = FastAPI(title="API с логированием в ClickHouse", lifespan=lifespan)


@app.middleware("http")
async def log_requests(request: Request, call_next):

    if request.url.path in ["/health", "/docs", "/openapi.json", "/favicon.ico", "/redoc"]:
        return await call_next(request)

    start_time = time.time()


    request_body = None
    try:
        if request.method in ["POST", "PUT", "PATCH"] and request.headers.get("content-length"):
            body = await request.body()
            request_body = body.decode('utf-8')[:1000] if body else None

            async def receive():
                return {"type": "http.request", "body": body, "more_body": False}
            request._receive = receive
    except Exception:
        request_body = None


    response = await call_next(request)


    process_time = (time.time() - start_time) * 1000


    log_data = {
        'level': 'ERROR' if response.status_code >= 400 else 'INFO',
        'service': 'points-api',
        'endpoint': request.url.path,
        'method': request.method,
        'status_code': response.status_code,
        'client_ip': request.client.host if request.client else None,
        'user_agent': request.headers.get('user-agent'),
        'request_duration_ms': round(process_time, 2),
        'request_body': request_body,
        'response_body': None,
        'error_message': None,
        'user_id': None,
        'params': dict(request.query_params) if request.query_params else None,
    }


    try:
        import asyncio
        asyncio.create_task(clickhouse_client.insert_log(log_data))
    except Exception as e:
        logger.error(f"Failed to send log to ClickHouse: {e}")


    logger.info(
        f"{request.method} {request.url.path} {response.status_code} "
        f"- {process_time:.2f}ms - IP: {request.client.host if request.client else 'Unknown'}"
    )

    return response


@app.get("/health")
async def check():
    return {"health": "ok", "clickhouse": "connected" if clickhouse_client._connected else "disconnected"}


@app.get("/page", response_class=HTMLResponse)
async def get_page():
    """
    Возвращает HTML страницу дашборда.
    HTML код читается из файла test_page.html.
    """
    try:
        # Путь к файлу (в той же директории, что и main.py)
        file_path = os.path.join(os.path.dirname(__file__), "test_page.html")

        # Читаем файл с HTML
        with open(file_path, "r", encoding="utf-8") as f:
            html_content = f.read()

        # Логируем запрос
        await log_db_operation(
            operation="GET_PAGE",
            table="html_page",
            data={"file": "test_page.html"}
        )

        return HTMLResponse(content=html_content)

    except FileNotFoundError:
        await log_db_operation(
            operation="GET_PAGE",
            table="html_page",
            data={"file": "test_page.html"},
            error="HTML file not found"
        )
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="HTML файл test_page.html не найден"
        )

    except Exception as e:
        await log_db_operation(
            operation="GET_PAGE",
            table="html_page",
            data={"file": "test_page.html"},
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при чтении HTML файла: {str(e)}"
        )


@app.get("/logs")
async def get_logs(
    limit: int = 100,
    level: Optional[str] = None,
    endpoint: Optional[str] = None,
    start_date: Optional[datetime] = None,
    end_date: Optional[datetime] = None
):
    """Получение логов из ClickHouse (для администраторов)"""
    logs = await clickhouse_client.get_logs(
        limit=limit,
        level=level,
        endpoint=endpoint,
        start_date=start_date,
        end_date=end_date
    )

    return {"logs": logs}



@app.get("/points", response_model=list[Point])
async def get_points(session: AsyncSession = Depends(get_session)):
    try:
        result = await session.exec(select(Point))
        points = result.all()


        await log_db_operation(
            operation="SELECT",
            table="points",
            data={"count": len(points)}
        )

        return [
            Point(
                home_num=point.home_num,
                volts=point.volts,
                ampers=point.ampers,
                power=point.power,
                resistance=point.resistance,
                home_id=point.home_id,
            )
            for point in points
        ]
    except Exception as e:
        await log_db_operation(
            operation="SELECT",
            table="points",
            error=str(e)
        )
        raise


@app.post("/create_point")
async def add_point(point: PointCreate, session: AsyncSession = Depends(get_session)):
    try:
        point_data = point.dict()
        point_obj = Point(
            home_num=point.home_num,
            volts=point.volts,
            ampers=point.ampers,
            power=point.power,
            resistance=point.resistance,
        )

        logger.info(f"Point data created: {point_obj}")
        session.add(point_obj)
        await session.commit()
        await session.refresh(point_obj)


        await log_db_operation(
            operation="INSERT",
            table="points",
            data=point_data
        )

        return point_obj
    except Exception as e:
        await log_db_operation(
            operation="INSERT",
            table="points",
            data=point.dict(),
            error=str(e)
        )
        raise

@app.get("/get_point", response_model=Point)
async def get_point(
    point_query: PointGet = Depends(),
    session: AsyncSession = Depends(get_session),
):
    """
    Получить точку по home_id или home_num.
    если указан home_id, ищет по нему, иначе по home_num.
    """
    try:

        if not point_query.home_num and point_query.home_id is None:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="Требуется хотя бы один параметр: home_id или home_num"
            )


        if point_query.home_id is not None:
            result = await session.exec(
                select(Point).where(Point.home_id == point_query.home_id)
            )
            point = result.one_or_none()

            if not point:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Точка с home_id={point_query.home_id} не найдена"
                )

            await log_db_operation(
                operation="SELECT",
                table="points",
                data={"home_id": point_query.home_id}
            )

            return point


        elif point_query.home_num:
            result = await session.exec(
                select(Point).where(Point.home_num == point_query.home_num)
            )
            point = result.one_or_none()

            if not point:
                raise HTTPException(
                    status_code=status.HTTP_404_NOT_FOUND,
                    detail=f"Участок с home_num={point_query.home_num} не найден"
                )

            await log_db_operation(
                operation="SELECT",
                table="points",
                data={"home_num": point_query.home_num}
            )

            return point

    except HTTPException:
        raise
    except Exception as e:
        await log_db_operation(
            operation="SELECT",
            table="points",
            data={
                "home_id": point_query.home_id,
                "home_num": point_query.home_num
            },
            error=str(e)
        )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=f"Ошибка при получении точки: {str(e)}"
        )


@app.post("/update_point/{home_id}", response_model=Point)
async def update_point(
    home_id: int,
    point_update: PointUpdate,
    session: AsyncSession = Depends(get_session),
):
    try:
        result = await session.exec(select(Point).where(Point.home_id == home_id))
        point = result.one_or_none()

        if not point:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND,
                detail=f"Sorry, point with ID {home_id} not found",
            )

        logger.info(f"Point with id {home_id} data updated: {point}")

        update_data = point_update.model_dump(exclude_unset=True)
        for field, value in update_data.items():
            if value is not None:
                setattr(point, field, value)

        session.add(point)
        await session.commit()
        await session.refresh(point)


        await log_db_operation(
            operation="UPDATE",
            table="points",
            data={"home_id": home_id, "updates": update_data}
        )

        return point
    except Exception as e:
        await log_db_operation(
            operation="UPDATE",
            table="points",
            data={"home_id": home_id, "updates": point_update.dict()},
            error=str(e)
        )
        raise


@app.delete("/delete_all_points")
async def delete_all_points(session: AsyncSession = Depends(get_session)):
    try:
        points = await session.exec(select(Point))
        points = points.all()
        if points:
            for point in points:
                await  session.delete(point)

            await session.commit()


            await log_db_operation(
                operation="DELETE",
                table="points",
                data={"count": len(points), "type": "all"}
            )
        else:
            raise HTTPException(
                status_code=status.HTTP_404_NOT_FOUND, detail="Points not found"
            )

        return {"points_delete_success": 200}
    except Exception as e:
        await log_db_operation(
            operation="DELETE",
            table="points",
            data={"type": "all"},
            error=str(e)
        )
        raise


@app.post("/create_points_excel")
async def add_points_excel(
    file: UploadFile, session: AsyncSession = Depends(get_session)
):
    try:
        content = await file.read()
        df = pd.read_excel(BytesIO(content))
        points = df.to_dict()
        print("DICT")
        print(points)
        points = normalize_dict_to_list(points)

        created_count = 0
        for point in points[1:]:
            point_obj = Point(
                home_num=point[0],
                volts=point[1],
                ampers=point[2],
                power=point[3],
                resistance=point[4],
            )
            session.add(point_obj)
            created_count += 1

        await session.commit()


        await log_db_operation(
            operation="BULK_INSERT",
            table="points",
            data={"source": "excel", "count": created_count, "filename": file.filename}
        )

        return {"file_data_uploaded": 200, "points_created": created_count}
    except Exception as e:
        await log_db_operation(
            operation="BULK_INSERT",
            table="points",
            data={"source": "excel", "filename": file.filename},
            error=str(e)
        )
        raise



@app.get("/find_paradox")
async def analyze_data(session: AsyncSession = Depends(get_session)):
    try:
        data = await session.exec(select(Point))
        points = data.all()
        points = [point.model_dump() for point in points]
        points = extract_home_data(points)
        paradox_point = analyze_points(points)
        print(points)
        print(paradox_point)


        await log_db_operation(
            operation="ANALYZE",
            table="points",
            data={"analysis_type": "paradox", "points_count": len(points)}
        )

        return paradox_point

    except Exception as e:
        await log_db_operation(
            operation="ANALYZE",
            table="points",
            data={"analysis_type": "paradox"},
            error=str(e)
        )
        raise



async def log_db_operation(
    operation: str,
    table: str,
    data: dict = None,
    error: str = None
):
    """Логирование операций с базой данных"""
    log_data = {
        'level': 'ERROR' if error else 'INFO',
        'service': 'database',
        'endpoint': f'db.{table}',
        'method': operation.upper(),
        'status_code': 500 if error else 200,
        'client_ip': None,
        'user_agent': None,
        'request_duration_ms': None,
        'request_body': json.dumps(data) if data else None,
        'response_body': None,
        'error_message': error,
        'user_id': None,
        'params': {'table': table, 'operation': operation},
    }

    try:
        import asyncio
        asyncio.create_task(clickhouse_client.insert_log(log_data))
    except Exception as e:
        logger.error(f"Failed to send DB log to ClickHouse: {e}")