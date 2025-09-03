"""
Database Interface Classes for Performance Testing
Only basic database operations without ML or complex business logic
"""
import asyncio
import asyncpg
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional
from abc import ABC, abstractmethod

class DatabaseInterface(ABC):
    """Base class for database interfaces"""
    
    def __init__(self, name: str):
        self.name = name
        self.metrics = {
            'insert_times': [],
            'update_times': [],
            'query_times': []
        }
    
    def reset_metrics(self):
        """Reset metrics to avoid accumulation between tests"""
        self.metrics = {
            'insert_times': [],
            'update_times': [],
            'query_times': []
        }
    
    @abstractmethod
    async def connect(self):
        pass
    
    @abstractmethod
    async def disconnect(self):
        pass
    
    @abstractmethod
    async def create_schema(self):
        pass
    
    @abstractmethod
    async def insert_trade(self, trade_data: Dict[str, Any]) -> bool:
        pass
    
    @abstractmethod
    async def insert_trades_batch(self, trades_data: List[Dict[str, Any]]) -> bool:
        pass
    
    @abstractmethod
    async def update_account_balance(self, account_id: int, new_balance: float) -> float:
        pass
    
    # Simple database query methods
    @abstractmethod
    async def get_account_balance(self, account_id: int) -> float:
        pass
    
    @abstractmethod
    async def count_account_trades(self, account_id: int) -> int:
        pass
    
    @abstractmethod
    async def get_recent_trades(self, account_id: int, start_time) -> List[Dict]:
        pass
    
    @abstractmethod
    async def count_trades_in_timerange(self, start_time, end_time) -> int:
        pass
    
    @abstractmethod
    async def get_account_trade_volume(self, account_id: int) -> float:
        pass

class QuestDBInterface(DatabaseInterface):
    def __init__(self):
        super().__init__("QuestDB")
        self.pool = None
    
    async def connect(self):
        """Connect to QuestDB using PostgreSQL wire protocol"""
        from config_loader import get_database_config
        config = get_database_config('questdb')
        
        try:
            # Increase pool size for highly concurrent tests (Test 4 uses 12 workers)
            self.pool = await asyncpg.create_pool(
                host=config['host'],
                port=config['port'],
                user=config['user'],
                password=config['password'],
                database=config['database'],
                min_size=5,
                max_size=15  # Enough for 12 concurrent workers + overhead
            )
        except Exception as e:
            raise ConnectionError(f"Failed to connect to QuestDB: {e}")
    
    async def disconnect(self):
        """Disconnect from QuestDB"""
        if self.pool:
            await self.pool.close()
    
    async def create_schema(self):
        """Create necessary tables"""
        async with self.pool.acquire() as conn:
            try:
                # Drop existing non-partitioned table if it exists
                try:
                    await conn.execute("DROP TABLE IF EXISTS trades")
                except:
                    pass
                
                # Create trades table with partitioning by day
                await conn.execute("""
                    CREATE TABLE trades (
                        trade_id LONG,
                        account_id INT,
                        symbol SYMBOL,
                        volume DOUBLE,
                        price DOUBLE,
                        trade_type SYMBOL,
                        timestamp TIMESTAMP
                    ) TIMESTAMP(timestamp) PARTITION BY DAY
                """)
                
                # Create indexes for performance (QuestDB doesn't support IF NOT EXISTS for indexes)
                try:
                    await conn.execute("CREATE INDEX idx_trades_account_id ON trades (account_id)")
                except:
                    pass  # Index might already exist
                try:
                    await conn.execute("CREATE INDEX idx_trades_timestamp ON trades (timestamp)")
                except:
                    pass  # Index might already exist
                try:
                    await conn.execute("CREATE INDEX idx_trades_account_timestamp ON trades (account_id, timestamp)")
                except:
                    pass  # Index might already exist
                
                # Create accounts table (use IF NOT EXISTS instead of DROP+CREATE)
                await conn.execute("""
                    CREATE TABLE IF NOT EXISTS accounts (
                        account_id INT,
                        balance DOUBLE,
                        updated_at TIMESTAMP
                    ) TIMESTAMP(updated_at) PARTITION BY DAY
                """)
                
                # Create indexes for accounts (skip IF NOT EXISTS as it might cause QuestDB syntax issues)
                try:
                    await conn.execute("CREATE INDEX idx_accounts_account_id ON accounts (account_id)")
                except:
                    pass  # Index might already exist
                try:
                    await conn.execute("CREATE INDEX idx_accounts_updated_at ON accounts (updated_at)")
                except:
                    pass  # Index might already exist
                
            except Exception as e:
                raise Exception(f"QuestDB schema creation failed: {e}")
    
    async def insert_trade(self, trade_data: Dict[str, Any]) -> bool:
        """Insert single trade"""
        start_time = time.time()
        try:
            async with self.pool.acquire() as conn:
                await conn.execute("""
                    INSERT INTO trades (trade_id, account_id, symbol, volume, price, trade_type, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                """, 
                trade_data['trade_id'],
                trade_data['account_id'],
                trade_data['symbol'],
                trade_data['volume'],
                trade_data['price'],
                trade_data['trade_type'],
                trade_data['timestamp']
                )
            
            self.metrics['insert_times'].append(time.time() - start_time)
            return True
            
        except Exception as e:
            raise Exception(f"Insert failed: {e}")
            return False
    
    async def insert_trades_batch(self, trades_data: List[Dict[str, Any]]) -> bool:
        """Insert multiple trades in batch"""
        if not trades_data:
            return True
            
        start_time = time.time()
        try:
            async with self.pool.acquire() as conn:
                # Use executemany for batch insert
                await conn.executemany("""
                    INSERT INTO trades (trade_id, account_id, symbol, volume, price, trade_type, timestamp)
                    VALUES ($1, $2, $3, $4, $5, $6, $7)
                """, [
                    (t['trade_id'], t['account_id'], t['symbol'], 
                     t['volume'], t['price'], t['trade_type'], t['timestamp'])
                    for t in trades_data
                ])
            
            self.metrics['insert_times'].append(time.time() - start_time)
            return True
            
        except Exception as e:
            raise Exception(f"Batch insert failed: {e}")
            return False
    
    async def update_account_balance(self, account_id: int, new_balance: float) -> float:
        """Update account balance"""
        start_time = time.time()
        try:
            async with self.pool.acquire() as conn:
                # QuestDB: Time-series approach - INSERT new record with timestamp
                # This is correct for QuestDB architecture (append-only with timestamps)
                await conn.execute("""
                    INSERT INTO accounts (account_id, balance, updated_at)
                    VALUES ($1, $2, $3)
                """, account_id, new_balance, datetime.now())
            
            self.metrics['update_times'].append(time.time() - start_time)
            return new_balance
            
        except Exception as e:
            raise Exception(f"Update failed: {e}")
            return 0.0
    
    async def get_account_balance(self, account_id: int) -> float:
        """Get account balance"""
        start_time = time.time()
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT balance FROM accounts 
                    WHERE account_id = $1 
                    ORDER BY updated_at DESC 
                    LIMIT 1
                """, account_id)
            
            self.metrics['query_times'].append(time.time() - start_time)
            return float(row['balance']) if row else 0.0
            
        except Exception as e:
            raise Exception(f"Failed to get account balance: {e}")
    
    async def count_account_trades(self, account_id: int) -> int:
        """Count trades for account"""
        start_time = time.time()
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT COUNT(*) as count FROM trades WHERE account_id = $1
                """, account_id)
            
            self.metrics['query_times'].append(time.time() - start_time)
            return int(row['count']) if row else 0
            
        except Exception as e:
            raise Exception(f"Failed to count account trades: {e}")
    
    async def get_recent_trades(self, account_id: int, start_time) -> List[Dict]:
        """Get recent trades for account"""
        query_start = time.time()
        try:
            async with self.pool.acquire() as conn:
                rows = await conn.fetch("""
                    SELECT trade_id, symbol, volume, price, trade_type, timestamp 
                    FROM trades 
                    WHERE account_id = $1 AND timestamp > $2
                    ORDER BY timestamp DESC
                    LIMIT 100
                """, account_id, start_time)
            
            self.metrics['query_times'].append(time.time() - query_start)
            return [dict(row) for row in rows]
            
        except Exception as e:
            raise Exception(f"Failed to get recent trades: {e}")
    
    async def count_trades_in_timerange(self, start_time, end_time) -> int:
        """Count trades in time range"""
        query_start = time.time()
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT COUNT(*) as count FROM trades 
                    WHERE timestamp BETWEEN $1 AND $2
                """, start_time, end_time)
            
            self.metrics['query_times'].append(time.time() - query_start)
            return int(row['count']) if row else 0
            
        except Exception as e:
            raise Exception(f"Failed to count trades in timerange: {e}")
    
    async def get_account_trade_volume(self, account_id: int) -> float:
        """Get total trade volume for account"""
        query_start = time.time()
        try:
            async with self.pool.acquire() as conn:
                row = await conn.fetchrow("""
                    SELECT SUM(volume) as total_volume FROM trades 
                    WHERE account_id = $1
                """, account_id)
            
            self.metrics['query_times'].append(time.time() - query_start)
            return float(row['total_volume']) if row and row['total_volume'] else 0.0
            
        except Exception as e:
            raise Exception(f"Failed to get account trade volume: {e}")

class ClickHouseInterface(DatabaseInterface):
    def __init__(self):
        super().__init__("ClickHouse")
        self.clients = []  # Simple connection pool
        self.client_index = 0
        self.connection_lock = None
    
    async def connect(self):
        """Connect to ClickHouse"""
        import asyncio
        from clickhouse_driver import Client
        from config_loader import get_database_config
        
        config = get_database_config('clickhouse')
        
        # Create connection lock for thread safety in concurrent tests
        self.connection_lock = asyncio.Lock()
        
        try:
            # Create connection pool for fair comparison with other databases
            pool_size = 10
            for i in range(pool_size):
                client = Client(
                    host=config['host'],
                    port=config['port'],
                    user=config['user'],
                    password=config['password'],
                    database=config['database'],
                    settings={
                        'max_execution_time': 300,  # 5 minutes
                        'connect_timeout': 60,
                        'send_timeout': 300,
                        'receive_timeout': 300
                    }
                )
                # Verify database connectivity
                client.execute('SELECT 1')
                self.clients.append(client)
            
            # Keep first client for backward compatibility
            self.client = self.clients[0]
            
        except Exception as e:
            raise ConnectionError(f"Failed to connect to ClickHouse: {e}")
    
    def _get_client(self):
        """Get next client from pool (round-robin)"""
        if not self.clients:
            return self.client
        client = self.clients[self.client_index % len(self.clients)]
        self.client_index += 1
        return client
    
    async def disconnect(self):
        """Disconnect from ClickHouse"""
        for client in self.clients:
            try:
                client.disconnect()
            except:
                pass
        if self.client:
            self.client.disconnect()
    
    async def create_schema(self):
        """Create necessary tables"""
        try:
            # Create trades table with proper ordering for performance
            client = self._get_client()
            client.execute("""
                CREATE TABLE IF NOT EXISTS trades (
                    trade_id UInt64,
                    account_id UInt32,
                    symbol String,
                    volume Float64,
                    price Float64,
                    trade_type String,
                    timestamp DateTime
                ) ENGINE = MergeTree()
                PARTITION BY toYYYYMM(timestamp)
                ORDER BY (account_id, timestamp)
                SETTINGS index_granularity = 8192
            """)
            
            # Create accounts table
            client = self._get_client()
            client.execute("""
                CREATE TABLE IF NOT EXISTS accounts (
                    account_id UInt32,
                    balance Float64,
                    updated_at DateTime
                ) ENGINE = ReplacingMergeTree(updated_at)
                ORDER BY account_id
            """)
            
            # Add performance indexes for ClickHouse
            # Note: ClickHouse uses MergeTree ORDER BY for primary indexing
            # Additional secondary indexes can be created if needed
            
        except Exception:
            pass
    
    async def insert_trade(self, trade_data: Dict[str, Any]) -> bool:
        """Insert single trade"""
        start_time = time.time()
        try:
            client = self._get_client()
            client.execute("""
                INSERT INTO trades (trade_id, account_id, symbol, volume, price, trade_type, timestamp)
                VALUES
            """, [trade_data])
            
            self.metrics['insert_times'].append(time.time() - start_time)
            return True
            
        except Exception as e:
            raise Exception(f"Insert failed: {e}")
            return False
    
    async def insert_trades_batch(self, trades_data: List[Dict[str, Any]]) -> bool:
        """Insert multiple trades in batch"""
        if not trades_data:
            return True
            
        start_time = time.time()
        try:
            # Optimize batch insert for ClickHouse - use larger batches
            batch_size = 1000
            for i in range(0, len(trades_data), batch_size):
                batch = trades_data[i:i + batch_size]
                client = self._get_client()
                client.execute("""
                    INSERT INTO trades (trade_id, account_id, symbol, volume, price, trade_type, timestamp)
                    VALUES
                """, batch)
            
            self.metrics['insert_times'].append(time.time() - start_time)
            return True
            
        except Exception as e:
            raise Exception(f"Batch insert failed: {e}")
            return False
    
    async def update_account_balance(self, account_id: int, new_balance: float) -> float:
        """Update account balance"""
        start_time = time.time()
        try:
            # ClickHouse: ReplacingMergeTree - INSERT overwrites by key
            # This is correct for ClickHouse architecture
            client = self._get_client()
            client.execute("""
                INSERT INTO accounts (account_id, balance, updated_at)
                VALUES
            """, [{'account_id': account_id, 'balance': new_balance, 'updated_at': datetime.now()}])
            
            self.metrics['update_times'].append(time.time() - start_time)
            return new_balance
            
        except Exception as e:
            raise Exception(f"Update failed: {e}")
            return 0.0
    
    async def get_account_balance(self, account_id: int) -> float:
        """Get account balance"""
        start_time = time.time()
        try:
            client = self._get_client()
            result = client.execute(
                f"SELECT balance FROM accounts WHERE account_id = {account_id} ORDER BY updated_at DESC LIMIT 1"
            )
            
            self.metrics['query_times'].append(time.time() - start_time)
            return float(result[0][0]) if result else 0.0
            
        except Exception as e:
            raise Exception(f"Failed to get account balance: {e}")
    
    async def count_account_trades(self, account_id: int) -> int:
        """Count trades for account"""
        start_time = time.time()
        try:
            client = self._get_client()
            result = client.execute(
                f"SELECT COUNT(*) FROM trades WHERE account_id = {account_id}"
            )
            
            self.metrics['query_times'].append(time.time() - start_time)
            return int(result[0][0]) if result else 0
            
        except Exception as e:
            raise Exception(f"Failed to count account trades: {e}")
    
    async def get_recent_trades(self, account_id: int, start_time) -> List[Dict]:
        """Get recent trades for account"""
        query_start = time.time()
        try:
            # Format datetime for ClickHouse
            formatted_start_time = start_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(start_time, 'strftime') else start_time
            client = self._get_client()
            result = client.execute(
                f"SELECT trade_id, symbol, volume, price, trade_type, timestamp FROM trades WHERE account_id = {account_id} AND timestamp > '{formatted_start_time}' ORDER BY timestamp DESC LIMIT 100"
            )
            
            self.metrics['query_times'].append(time.time() - query_start)
            return [{'trade_id': r[0], 'symbol': r[1], 'volume': r[2], 
                    'price': r[3], 'trade_type': r[4], 'timestamp': r[5]} for r in result]
            
        except Exception as e:
            raise Exception(f"Failed to get recent trades: {e}")
    
    async def count_trades_in_timerange(self, start_time, end_time) -> int:
        """Count trades in time range"""
        query_start = time.time()
        try:
            # Format datetime for ClickHouse
            formatted_start_time = start_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(start_time, 'strftime') else start_time
            formatted_end_time = end_time.strftime('%Y-%m-%d %H:%M:%S') if hasattr(end_time, 'strftime') else end_time
            client = self._get_client()
            result = client.execute(
                f"SELECT COUNT(*) FROM trades WHERE timestamp BETWEEN '{formatted_start_time}' AND '{formatted_end_time}'"
            )
            
            self.metrics['query_times'].append(time.time() - query_start)
            return int(result[0][0]) if result else 0
            
        except Exception as e:
            raise Exception(f"Failed to count trades in timerange: {e}")
    
    async def get_account_trade_volume(self, account_id: int) -> float:
        """Get total trade volume for account"""
        query_start = time.time()
        try:
            client = self._get_client()
            result = client.execute(
                f"SELECT SUM(volume) FROM trades WHERE account_id = {account_id}"
            )
            
            self.metrics['query_times'].append(time.time() - query_start)
            return float(result[0][0]) if result and result[0][0] else 0.0
            
        except Exception as e:
            raise Exception(f"Failed to get account trade volume: {e}")
