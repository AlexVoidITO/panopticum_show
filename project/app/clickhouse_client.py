from clickhouse_driver import Client
from clickhouse_driver.errors import Error
import asyncio
from datetime import datetime
from typing import Dict, Any, Optional, List
import json
import logging


import os 


logger = logging.getLogger(__name__)


class ClickHouseClient:
    def __init__(
        self,
        host: str = "",
        port: int = ,
        database: str = "",
        username: str = "",
        password: str = "",
    ):
        self.host = host
        self.port = port
        self.database = database
        self.username = username
        self.password = password
        self.client = None
        self._connected = False

    async def connect(self):
        """Асинхронное подключение к ClickHouse"""
        try:
            loop = asyncio.get_event_loop()
            self.client = await loop.run_in_executor(
                None,
                lambda: Client(
                    host=self.host,
                    port=self.port,
                    database=self.database,
                    user=self.username,
                    password=self.password,
                    settings={'use_numpy': False},
                )
            )
            self._connected = True
            logger.info(f"Connected to ClickHouse at {self.host}:{self.port}")
            

            await self._create_logs_table()
            
        except Error as e:
            logger.error(f"Error connecting to ClickHouse: {e}")
            self._connected = False


    async def _create_logs_table(self):
        """Создание таблицы для логов"""
        create_table_query = """
        CREATE TABLE IF NOT EXISTS api_logs
        (
            log_id UUID DEFAULT generateUUIDv4(),
            timestamp DateTime DEFAULT now(),
            level String,
            service String,
            endpoint String,
            method String,
            status_code Nullable(Int32),
            client_ip Nullable(String),
            user_agent Nullable(String),
            request_duration_ms Nullable(Float64),
            request_body Nullable(String),
            response_body Nullable(String),
            error_message Nullable(String),
            user_id Nullable(String),
            params Nullable(String),
            created_at DateTime DEFAULT now()
        )
        ENGINE = MergeTree()
        ORDER BY (timestamp, service, level)
        PARTITION BY toYYYYMM(timestamp)
        TTL created_at + INTERVAL 30 DAY
        SETTINGS index_granularity = 8192;
        """
        
        try:
            await self.execute(create_table_query)
            logger.info("Logs table created or already exists")
        except Error as e:
            logger.error(f"Error creating logs table: {e}")

    async def execute(self, query: str, params: Optional[Dict] = None):
        """Выполнение запроса к ClickHouse"""
        if not self._connected or not self.client:
            logger.warning("ClickHouse client is not connected")
            return None
        
        try:
            loop = asyncio.get_event_loop()
            return await loop.run_in_executor(
                None,
                lambda: self.client.execute(query, params) if params else self.client.execute(query)
            )
        except Error as e:
            logger.error(f"Error executing query in ClickHouse: {e}")
            return None

    async def insert_log(self, log_data: Dict[str, Any]):
        """Вставка лога в ClickHouse"""
        if not self._connected:
            logger.debug("ClickHouse not connected, skipping log")
            return
        
        insert_query = """
        INSERT INTO api_logs 
        (level, service, endpoint, method, status_code, client_ip, user_agent, 
         request_duration_ms, request_body, response_body, error_message, user_id, params)
        VALUES
        """
        
        try:

            prepared_params = {
                'level': log_data.get('level', 'INFO'),
                'service': log_data.get('service', 'unknown'),
                'endpoint': log_data.get('endpoint', ''),
                'method': log_data.get('method', 'GET'),
                'status_code': log_data.get('status_code'),
                'client_ip': log_data.get('client_ip'),
                'user_agent': log_data.get('user_agent'),
                'request_duration_ms': log_data.get('request_duration_ms'),
                'request_body': log_data.get('request_body'),
                'response_body': log_data.get('response_body'),
                'error_message': log_data.get('error_message'),
                'user_id': log_data.get('user_id'),
                'params': json.dumps(log_data.get('params')) if log_data.get('params') else None,
            }
            

            await self.execute(
                insert_query + " (%(level)s, %(service)s, %(endpoint)s, %(method)s, %(status_code)s, "
                "%(client_ip)s, %(user_agent)s, %(request_duration_ms)s, %(request_body)s, "
                "%(response_body)s, %(error_message)s, %(user_id)s, %(params)s)",
                prepared_params
            )
            
        except Error as e:
            logger.error(f"Error inserting log to ClickHouse: {e}")

    async def get_logs(
        self, 
        limit: int = 100,
        level: Optional[str] = None,
        endpoint: Optional[str] = None,
        start_date: Optional[datetime] = None,
        end_date: Optional[datetime] = None
    ) -> List[tuple]:
        """Получение логов из ClickHouse"""
        if not self._connected:
            logger.warning("ClickHouse not connected")
            return []
        
        query = """
        SELECT 
            log_id,
            timestamp,
            level,
            service,
            endpoint,
            method,
            status_code,
            client_ip,
            user_agent,
            request_duration_ms,
            request_body,
            response_body,
            error_message,
            user_id,
            params
        FROM api_logs 
        WHERE 1=1
        """
        
        params = {}
        
        if level:
            query += " AND level = %(level)s"
            params['level'] = level
        
        if endpoint:
            query += " AND endpoint LIKE %(endpoint)s"
            params['endpoint'] = f"%{endpoint}%"
        
        if start_date:
            query += " AND timestamp >= %(start_date)s"
            params['start_date'] = start_date
        
        if end_date:
            query += " AND timestamp <= %(end_date)s"
            params['end_date'] = end_date
        
        query += " ORDER BY timestamp DESC LIMIT %(limit)s"
        params['limit'] = limit
        
        try:
            result = await self.execute(query, params)
            return result if result else []
        except Error as e:
            logger.error(f"Error getting logs from ClickHouse: {e}")
            return []

    async def disconnect(self):
        """Отключение от ClickHouse"""
        if self.client and self._connected:
            try:
                loop = asyncio.get_event_loop()
                await loop.run_in_executor(None, self.client.disconnect)
                self._connected = False
                logger.info("Disconnected from ClickHouse")
            except Error as e:
                logger.error(f"Error disconnecting from ClickHouse: {e}")



clickhouse_client = ClickHouseClient(
    host="",  
    port=,
    database=os.environ.get("CLICKHOUSE_DB"),
    username=os.environ.get("CLICKHOUSE_USER"),
    password=os.environ.get("CLICKHOUSE_PASSWORD")
)