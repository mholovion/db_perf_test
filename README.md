# Anti-Abuse Database Performance Test Suite

Database performance testing framework.

## How to Run Tests

### Step 1: Install Dependencies
```bash
pip install -r requirements.txt
```

### Step 2: Generate Docker Compose Configuration
```bash
python generate_compose.py
```

### Step 3: Start Database Services  
```bash
docker-compose up -d
```

### Step 4: Run Performance Tests
```bash
python test_executor.py
```

### Step 5: Stop Services (when done)
```bash
docker-compose down
```

## Test Cases

| Test | Name | Description | Category | Importance |
|------|------|-------------|----------|------------|
| 1 | High-Frequency Time-Series Inserts | Simulates high-frequency trading data ingestion | Time-Series Ingestion | Critical |
| 2 | Time-Range Analytics | Time-series analytical queries over different time windows | Time-Series Analytics | High |
| 3 | Late-Arriving Data Handling | Tests handling of out-of-order time-series data | Out-of-Order Data | Medium |
| 4 | Concurrent Time-Series Streams | Multiple concurrent data streams writing simultaneously | Concurrent Ingestion | Critical |
| 5 | Daily Broker Load (50k trades) | Simulates medium size broker daily load - 50,000 trades per day | Broker Load Simulation | Critical |
| 6 | Anti-Abuse Interface Load | 10 concurrent users performing anti-abuse queries | Anti-Abuse Interface | Critical |
| 7 | Query Response Time Test | Validates individual database query response times for anti-abuse operations | Query Performance | Critical |
| 8 | Progressive Load Stress Test | Tests system stability under progressively increasing load with performance degradation analysis | Stress Testing | High |
| 9 | Resource Usage Monitoring | Tests database connection efficiency and resource management under varied workloads | Resource Monitoring | Medium |
| 10 | Bulk Update Performance Test | Tests database performance for bulk update operations with various batch sizes | Update Performance | High |

## Adding New Tests

To add new tests, follow these steps:

### 1. Create the test method in `all_tests.py`:

```python
async def test_case_11_your_test_name(self, db_name: str) -> Dict[str, Any]:
    """Your Test Description"""
    test_info = {
        'test_id': 11,  # Use next available test ID
        'name': 'Your Test Name',
        'description': 'Detailed description of what this test measures',
        'category': 'test_category',  # e.g., 'performance', 'stress_testing', 'concurrent_operations'
        'importance': 'high',  # critical|high|medium|low
        'relevance': 'Anti-abuse system relevance description'
    }
    
    # Get database interface
    db = self.databases[db_name]
    db.reset_metrics()
    
    try:
        start_time = time.time()
        
        # Your test logic here - examples:
        # await db.insert_trade(trade_data)
        # await db.update_account_balance(account_id, new_balance)
        # await db.get_account_balance(account_id)
        # await db.count_account_trades(account_id)
        
        total_time = time.time() - start_time
        
        # Return test results with success metrics
        return {
            **test_info,
            'success': True,
            'total_time': total_time,
            'primary_metric_value': 123.45,  # Your main performance metric
            # Add other relevant metrics here
        }
        
    except Exception as e:
        return {**test_info, 'success': False, 'error': str(e)}
```

### 2. Update CSV exporter metrics (if needed):

If your test uses a new primary metric, add it to `csv_exporter.py` in the `metric_mappings` dictionary:

```python
'your_metric_name': {'unit': ' units', 'higher_better': True, 'threshold': 100, 'precision': 2},
```

### 3. Update the priority metrics list:

Add your metric to the `priority_metrics` list in `csv_exporter.py` to ensure proper CSV output.

## Test Results

After running tests, results are exported to:
- `results/test_results.json` - Raw test data with detailed metrics
- `results/performance_analysis.csv` - Performance comparison and ratings
- `logs/test_suite.log` - Execution logs

### Performance Rating System
- **EXCELLENT (BEST)** - Top performer among tested databases
- **EXCELLENT (90%+)** - Within 90% performance of the best
- **GOOD (75-89%)** - Within 75-89% performance of the best  
- **AVERAGE (50-74%)** - Within 50-74% performance of the best
- **POOR (<50%)** - Below 50% performance of the best
- **FAILED** - Test failed to complete

## Supported Databases

- **QuestDB** - Time-series database optimized for high-frequency data
- **ClickHouse** - Column-oriented database for analytics

Both databases are configured with Docker containers for consistent testing environments.

## Adding New Database

To add a new database to the test suite, follow these steps:

### 1. Create database interface in `database_interfaces.py`:

```python
class YourDatabaseInterface(DatabaseInterface):
    def __init__(self, config: Dict[str, Any]):
        super().__init__("YourDatabase")
        self.config = config
        self.connection = None
        # Initialize your database connection
    
    async def connect(self):
        """Establish database connection"""
        # Your connection logic here
        pass
    
    async def disconnect(self):
        """Close database connection"""
        # Your disconnection logic here
        pass
    
    async def create_schema(self):
        """Create necessary tables/schema"""
        # Your schema creation logic
        pass
    
    # Implement required abstract methods:
    async def insert_trade(self, trade_data: Dict[str, Any]) -> bool:
        """Insert single trade record"""
        pass
    
    async def insert_trades_batch(self, trades: List[Dict[str, Any]]) -> bool:
        """Insert multiple trades in batch"""
        pass
    
    async def update_account_balance(self, account_id: int, new_balance: float) -> bool:
        """Update account balance"""
        pass
    
    async def get_account_balance(self, account_id: int) -> float:
        """Get account balance"""
        pass
    
    async def count_account_trades(self, account_id: int) -> int:
        """Count trades for account"""
        pass
    
    async def get_account_trade_volume(self, account_id: int) -> float:
        """Get total trade volume for account"""
        pass
    
    async def get_recent_trades(self, account_id: int, since: datetime) -> List[Dict]:
        """Get recent trades since timestamp"""
        pass
```

### 2. Add database configuration to `database_config.py`:

```python
DATABASE_CONFIGS = {
    # ... existing configs ...
    'YourDatabase': {
        'host': 'localhost',
        'port': 9000,
        'database': 'test_db',
        'user': 'default',
        'password': '',
        # Add your database-specific config
    }
}
```

### 3. Update Docker configuration in `generate_compose.py`:

Add your database service to the Docker Compose template:

```python
your_database_service = """
  your-database:
    image: your-database:latest
    container_name: your-database-test
    ports:
      - "9000:9000"
    environment:
      - YOUR_DB_CONFIG=value
    volumes:
      - your_db_data:/var/lib/your-database
    healthcheck:
      test: ["CMD", "your-db-health-check"]
      interval: 30s
      timeout: 10s
      retries: 3
"""
```

### 4. Register database in test framework:

Update the `self.databases` list in `test_executor.py`:

```python
self.databases = {
    'QuestDB': QuestDBInterface(),
    'ClickHouse': ClickHouseInterface(),
    'YourDatabase': YourDatabaseInterface()
}
```

### 5. Add initialization in `all_tests.py`:

Update the `DatabaseTester` class constructor to include your database:

```python
async def setup_databases(self):
    # ... existing database setup ...
    if 'YourDatabase' in self.database_names:
        self.databases['YourDatabase'] = YourDatabaseInterface(
            DATABASE_CONFIGS['YourDatabase']
        )
        await self.databases['YourDatabase'].connect()
        await self.databases['YourDatabase'].create_schema()
```

### Requirements:

All database interfaces must inherit from `DatabaseInterface(ABC)` and implement all abstract methods. The base class provides metrics tracking and standardized interface for consistent testing across different databases.
