"""
Time-Series Database Performance Tests
Optimized for QuestDB and ClickHouse time-series workloads
"""
import asyncio
import time
from datetime import datetime, timedelta
from typing import Dict, Any

class AllTests:
    """Time-Series Database Performance Tests - Main Test Suite"""
    
    def __init__(self, databases: Dict[str, Any], csv_exporter=None):
        self.databases = databases
        self.csv_exporter = csv_exporter
        
        # Standardized account ranges for all tests
        self.TEST_ACCOUNTS = {
            'main': list(range(5000, 6000)),      # 1000 accounts for most tests
            'small': list(range(5000, 5100)),     # 100 accounts for lighter tests  
            'bulk_update': list(range(5000, 5500)) # 500 accounts for update tests
        }
    
    async def cleanup_test_data(self, db_name: str):
        """Clean up all test data after each test to prevent memory/disk issues"""
        try:
            db = self.databases[db_name]
            
            # Get min and max account_id from all test account ranges
            all_accounts = []
            all_accounts.extend(self.TEST_ACCOUNTS['main'])
            all_accounts.extend(self.TEST_ACCOUNTS['small']) 
            all_accounts.extend(self.TEST_ACCOUNTS['bulk_update'])
            
            min_account = min(all_accounts)
            max_account = max(all_accounts)
            
            # Delete all test data from our account ranges
            trades_cleanup_sql = f"DELETE FROM trades WHERE account_id >= {min_account} AND account_id <= {max_account}"
            accounts_cleanup_sql = f"DELETE FROM accounts WHERE account_id >= {min_account} AND account_id <= {max_account}"
            
            if hasattr(db, 'execute'):
                # ClickHouse
                await db.execute(trades_cleanup_sql)
                try:
                    await db.execute(accounts_cleanup_sql)
                except:
                    pass  # accounts table might not exist yet
            elif hasattr(db, 'pool') and db.pool:
                # QuestDB  
                async with db.pool.acquire() as conn:
                    await conn.execute(trades_cleanup_sql)
                    try:
                        await conn.execute(accounts_cleanup_sql)
                    except:
                        pass  # accounts table might not exist yet
                    
        except Exception:
            # Cleanup failures are not critical for testing
            pass
    
    def _generate_time_series_data(self, trade_id: int, account_id: int, timestamp: datetime = None) -> Dict[str, Any]:
        """Generate deterministic time-series trade data"""
        symbols = ['EURUSD', 'GBPUSD', 'USDJPY', 'AUDUSD', 'USDCAD', 'USDCHF', 'NZDUSD']
        trade_types = ['BUY', 'SELL']
        
        return {
            'trade_id': trade_id,
            'account_id': account_id,
            'symbol': symbols[trade_id % len(symbols)],
            'volume': round(0.01 + (trade_id % 1000) / 100.0, 2),
            'price': round(0.5 + (trade_id % 300) / 200.0, 5),
            'trade_type': trade_types[trade_id % len(trade_types)],
            'timestamp': timestamp or datetime.now()
        }
    
    async def test_case_1_high_frequency_inserts(self, db_name: str) -> Dict[str, Any]:
        """High-Frequency Time-Series Inserts"""
        test_info = {
            'test_id': 1,
            'name': 'High-Frequency Time-Series Inserts',
            'description': 'Simulates high-frequency trading data ingestion',
            'category': 'time_series_ingestion',
            'importance': 'critical',
            'relevance': 'Real-time trade data streaming'
        }
        
        db = self.databases[db_name]
        db.reset_metrics()
        
        trades_per_second = 1000
        duration_seconds = 60
        total_trades = trades_per_second * duration_seconds
        
        start_time = time.time()
        base_timestamp = datetime.now() - timedelta(hours=1)
        
        try:
            batch_size = 1000
            trades_inserted = 0
            
            for batch_num in range(0, total_trades, batch_size):
                batch_trades = []
                for i in range(min(batch_size, total_trades - batch_num)):
                    trade_timestamp = base_timestamp + timedelta(
                        microseconds=(batch_num + i) * (1000000 // trades_per_second)
                    )
                    
                    account_id = self.TEST_ACCOUNTS['main'][((batch_num + i) % len(self.TEST_ACCOUNTS['main']))]
                    trade_data = self._generate_time_series_data(
                        batch_num + i + 1, 
                        account_id, 
                        trade_timestamp
                    )
                    batch_trades.append(trade_data)
                
                await db.insert_trades_batch(batch_trades)
                trades_inserted += len(batch_trades)
        
        except Exception as e:
            await self.cleanup_test_data(db_name)
            return {**test_info, 'success': False, 'error': str(e)}
        finally:
            await self.cleanup_test_data(db_name)
        
        total_time = time.time() - start_time
        
        return {
            **test_info,
            'success': True,
            'total_time': total_time,
            'trades_inserted': trades_inserted,
            'trades_per_second': trades_inserted / total_time,
            'avg_batch_time_ms': (sum(db.metrics['insert_times']) / len(db.metrics['insert_times']) * 1000) if db.metrics.get('insert_times') else 0
        }
    
    async def test_case_2_time_range_analytics(self, db_name: str) -> Dict[str, Any]:
        """Test 2: Time-Range Analytics (typical time-series queries)"""
        test_info = {
            'test_id': 2,
            'name': 'Time-Range Analytics',
            'description': 'Time-series analytical queries over different time windows',
            'category': 'time_series_analytics',
            'importance': 'high',
            'relevance': 'Historical data analysis and reporting'
        }
        
        db = self.databases[db_name]
        db.reset_metrics()
        
        # Setup: Create historical data spanning multiple days
        setup_start = time.time()
        historical_trades = []
        days_of_data = 7
        trades_per_day = 10000
        
        for day in range(days_of_data):
            for trade_num in range(trades_per_day):
                trade_timestamp = datetime.now() - timedelta(days=days_of_data-day) + timedelta(
                    seconds=trade_num * (86400 // trades_per_day)  # Distribute over 24 hours
                )
                
                account_id = self.TEST_ACCOUNTS['small'][((day * trades_per_day + trade_num) % len(self.TEST_ACCOUNTS['small']))]
                trade_data = self._generate_time_series_data(
                    day * trades_per_day + trade_num + 1,
                    account_id,
                    trade_timestamp
                )
                historical_trades.append(trade_data)
        
        await db.insert_trades_batch(historical_trades)
        setup_time = time.time() - setup_start
        
        db.reset_metrics()  # Reset after setup
        
        start_time = time.time()
        total_queries = 0
        
        try:
            # Query different time ranges for performance assessment
            time_ranges = [
                ('last_hour', timedelta(hours=1)),
                ('last_4_hours', timedelta(hours=4)),
                ('last_day', timedelta(days=1)),
                ('last_3_days', timedelta(days=3)),
                ('last_week', timedelta(days=7))
            ]
            
            for _, time_delta in time_ranges:
                for _ in range(20):
                    start_time_dt = datetime.now() - time_delta
                    end_time_dt = datetime.now()
                    
                    await db.count_trades_in_timerange(start_time_dt, end_time_dt)
                    total_queries += 1
        
        except Exception as e:
            await self.cleanup_test_data(db_name)
            return {**test_info, 'success': False, 'error': str(e)}
        finally:
            await self.cleanup_test_data(db_name)
        
        total_time = time.time() - start_time
        avg_query_time = sum(db.metrics['query_times']) / len(db.metrics['query_times']) if db.metrics.get('query_times') else 0
        
        return {
            **test_info,
            'success': True,
            'setup_time': setup_time,
            'total_time': total_time,
            'total_queries': total_queries,
            'queries_per_second': total_queries / total_time,
            'avg_query_time_ms': avg_query_time * 1000,
            'historical_trades_inserted': len(historical_trades)
        }

    async def test_case_3_late_arriving_data(self, db_name: str) -> Dict[str, Any]:
        """Test 3: Late-Arriving Data (out-of-order inserts)"""
        test_info = {
            'test_id': 3,
            'name': 'Late-Arriving Data Handling',
            'description': 'Tests handling of out-of-order time-series data',
            'category': 'out_of_order_data',
            'importance': 'medium',
            'relevance': 'Real-world data often arrives out of chronological order'
        }
        
        db = self.databases[db_name]
        db.reset_metrics()
        
        start_time = time.time()
        total_trades = 5000
        
        try:
            # Create trades with intentionally mixed timestamps
            mixed_trades = []
            base_time = datetime.now() - timedelta(minutes=30)
            
            for i in range(total_trades):
                # Deterministic 30% late trades pattern
                if (i % 10) < 3:  # 30% of trades are "late"
                    # Late data: 5-15 minutes old
                    timestamp = base_time - timedelta(minutes=5 + (i % 11))
                else:
                    # Recent data: within last 5 minutes
                    timestamp = base_time + timedelta(seconds=(i % 300))
                
                account_id = self.TEST_ACCOUNTS['small'][(i % len(self.TEST_ACCOUNTS['small']))]
                trade_data = self._generate_time_series_data(
                    i + 1,
                    account_id,
                    timestamp
                )
                mixed_trades.append(trade_data)
            
            # Insert in batches (simulating real-time mixed arrivals)
            batch_size = 500
            trades_inserted = 0
            
            for batch_start in range(0, total_trades, batch_size):
                batch = mixed_trades[batch_start:batch_start + batch_size]
                await db.insert_trades_batch(batch)
                trades_inserted += len(batch)
        
        except Exception as e:
            await self.cleanup_test_data(db_name)
            return {**test_info, 'success': False, 'error': str(e)}
        finally:
            await self.cleanup_test_data(db_name)
        
        total_time = time.time() - start_time
        
        return {
            **test_info,
            'success': True,
            'total_time': total_time,
            'trades_inserted': trades_inserted,
            'out_of_order_ratio': 0.3,
            'inserts_per_second': trades_inserted / total_time,
            'avg_insert_time_ms': (sum(db.metrics['insert_times']) / len(db.metrics['insert_times']) * 1000) if db.metrics.get('insert_times') else 0
        }

    async def test_case_4_concurrent_streams(self, db_name: str) -> Dict[str, Any]:
        """Test 4: Concurrent Data Streams (multiple simultaneous data sources)"""
        test_info = {
            'test_id': 4,
            'name': 'Concurrent Time-Series Streams',
            'description': 'Multiple concurrent data streams writing simultaneously',
            'category': 'concurrent_ingestion',
            'importance': 'critical',
            'relevance': 'Multiple trading venues feeding data simultaneously'
        }
        
        db = self.databases[db_name]
        db.reset_metrics()
        
        async def data_stream(stream_id: int, trades_count: int):
            """Simulate a single data stream"""
            stream_trades = []
            base_time = datetime.now()
            
            for i in range(trades_count):
                timestamp = base_time + timedelta(microseconds=i * 1000)
                account_id = self.TEST_ACCOUNTS['main'][(stream_id * 100 + i) % len(self.TEST_ACCOUNTS['main'])]
                
                trade_data = self._generate_time_series_data(
                    stream_id * 10000 + i + 1,
                    account_id,
                    timestamp
                )
                stream_trades.append(trade_data)
            
            # Insert stream data in batches
            batch_size = 1000
            for batch_start in range(0, len(stream_trades), batch_size):
                batch = stream_trades[batch_start:batch_start + batch_size]
                await db.insert_trades_batch(batch)
            
            return len(stream_trades)
        
        start_time = time.time()
        
        try:
            # Simulate 8 concurrent data streams
            streams = 8
            trades_per_stream = 2500
            
            stream_tasks = [
                asyncio.create_task(data_stream(stream_id, trades_per_stream))
                for stream_id in range(streams)
            ]
            
            results = await asyncio.gather(*stream_tasks, return_exceptions=True)
            
            total_trades = sum(r for r in results if isinstance(r, int))
            failed_streams = len([r for r in results if isinstance(r, Exception)])
        
        except Exception as e:
            await self.cleanup_test_data(db_name)
            return {**test_info, 'success': False, 'error': str(e)}
        finally:
            await self.cleanup_test_data(db_name)
        
        total_time = time.time() - start_time
        
        return {
            **test_info,
            'success': failed_streams == 0,
            'total_time': total_time,
            'concurrent_streams': streams,
            'total_trades_inserted': total_trades,
            'failed_streams': failed_streams,
            'aggregate_throughput': total_trades / total_time,
            'avg_stream_performance': (total_trades / streams) / total_time if streams > 0 else 0
        }
    
    async def test_case_5_daily_broker_load(self, db_name: str) -> Dict[str, Any]:
        """Test 5: Daily Broker Load - 50k trades per day"""
        test_info = {
            'test_id': 5,
            'name': 'Daily Broker Load (50k trades)',
            'description': 'Simulates medium size broker daily load - 50,000 trades per day',
            'category': 'broker_load_simulation',
            'importance': 'critical',
            'relevance': 'Real broker daily volume testing'
        }
        
        db = self.databases[db_name]
        db.reset_metrics()
        
        start_time = time.time()
        daily_trades = 50000
        
        # Distribute trades across 24 hours (simulate realistic trading patterns)
        # Peak hours: 8AM-4PM (70% of trades), Off-peak: remaining hours (30%)
        peak_trades = int(daily_trades * 0.7)  # 35,000 trades
        off_peak_trades = daily_trades - peak_trades  # 15,000 trades
        
        try:
            base_timestamp = datetime.now() - timedelta(hours=24)
            all_trades = []
            
            # Generate peak hour trades (8 hours, higher frequency)
            peak_start = base_timestamp + timedelta(hours=8)  # 8 AM
            for i in range(peak_trades):
                trade_timestamp = peak_start + timedelta(
                    seconds=(i * (8 * 3600)) / peak_trades  # Distribute over 8 hours
                )
                account_id = self.TEST_ACCOUNTS['main'][(i % len(self.TEST_ACCOUNTS['main']))]
                trade_data = self._generate_time_series_data(
                    i + 1,
                    account_id,
                    trade_timestamp
                )
                all_trades.append(trade_data)
            
            # Generate off-peak trades (16 hours, lower frequency)
            off_peak_hours = [0, 1, 2, 3, 4, 5, 6, 7, 16, 17, 18, 19, 20, 21, 22, 23]
            for i in range(off_peak_trades):
                hour_offset = off_peak_hours[i % len(off_peak_hours)]
                trade_timestamp = base_timestamp + timedelta(
                    hours=hour_offset,
                    minutes=(i % 60),
                    seconds=(i % 60)
                )
                account_id = self.TEST_ACCOUNTS['main'][((peak_trades + i) % len(self.TEST_ACCOUNTS['main']))]
                trade_data = self._generate_time_series_data(
                    peak_trades + i + 1,
                    account_id,
                    trade_timestamp
                )
                all_trades.append(trade_data)
            
            # Insert in realistic batches (simulating batch processing)
            batch_size = 2000
            trades_inserted = 0
            batch_times = []
            
            for batch_start in range(0, len(all_trades), batch_size):
                batch_time_start = time.time()
                batch = all_trades[batch_start:batch_start + batch_size]
                await db.insert_trades_batch(batch)
                batch_time = time.time() - batch_time_start
                batch_times.append(batch_time)
                trades_inserted += len(batch)
        
        except Exception as e:
            await self.cleanup_test_data(db_name)
            return {**test_info, 'success': False, 'error': str(e)}
        finally:
            await self.cleanup_test_data(db_name)
        
        total_time = time.time() - start_time
        avg_batch_time = sum(batch_times) / len(batch_times) if batch_times else 0
        
        return {
            **test_info,
            'success': True,
            'total_time': total_time,
            'trades_inserted': trades_inserted,
            'daily_volume': daily_trades,
            'peak_hour_ratio': 0.7,
            'avg_trades_per_second': trades_inserted / total_time,
            'avg_batch_time_sec': avg_batch_time,
            'batches_processed': len(batch_times)
        }

    async def test_case_6_concurrent_antiabuse_users(self, db_name: str) -> Dict[str, Any]:
        """Test 6: Anti-Abuse Interface - 10 Concurrent Users"""
        test_info = {
            'test_id': 6,
            'name': 'Anti-Abuse Interface Load',
            'description': '10 concurrent users performing anti-abuse queries',
            'category': 'antiabuse_interface',
            'importance': 'critical',
            'relevance': 'Anti-abuse system concurrent user load'
        }
        
        db = self.databases[db_name]
        db.reset_metrics()
        
        # Setup: Create test data for anti-abuse queries
        setup_start = time.time()
        test_accounts = list(range(6000, 6100))  # 100 test accounts
        trades_per_account = 500
        
        # Create historical data for testing
        historical_trades = []
        base_time = datetime.now() - timedelta(hours=2)
        
        for account_id in test_accounts:
            for i in range(trades_per_account):
                trade_timestamp = base_time + timedelta(
                    seconds=i * (7200 // trades_per_account)  # Distribute over 2 hours
                )
                trade_data = self._generate_time_series_data(
                    account_id * 1000 + i,
                    account_id,
                    trade_timestamp
                )
                historical_trades.append(trade_data)
        
        await db.insert_trades_batch(historical_trades)
        setup_time = time.time() - setup_start
        db.reset_metrics()
        
        async def antiabuse_user_simulation(user_id: int):
            """Simulate anti-abuse analyst user queries"""
            user_queries = 0
            user_start = time.time()
            
            # Deterministic workload pattern for consistent results
            queries_per_user = 15
            
            # Predefined query pattern - same for all users to ensure consistency
            query_pattern = [
                ('account_balance', 6000 + (user_id % 10)),  # Each user gets specific accounts
                ('trade_count', 6010 + (user_id % 10)),
                ('recent_trades', 6020 + (user_id % 10)),
                ('trade_volume', 6030 + (user_id % 10)),
                ('time_range_activity', None),  # Global query
                ('account_balance', 6040 + (user_id % 10)),
                ('trade_count', 6050 + (user_id % 10)),
                ('recent_trades', 6060 + (user_id % 10)),
                ('trade_volume', 6070 + (user_id % 10)),
                ('time_range_activity', None),
                ('account_balance', 6080 + (user_id % 10)),
                ('trade_count', 6090 + (user_id % 10)),
                ('recent_trades', 6000 + (user_id % 10)),
                ('trade_volume', 6010 + (user_id % 10)),
                ('account_balance', 6020 + (user_id % 10))
            ]
            
            for query_type, account_id in query_pattern[:queries_per_user]:
                try:
                    if query_type == 'account_balance':
                        await db.get_account_balance(account_id)
                    elif query_type == 'trade_count':
                        await db.count_account_trades(account_id)
                    elif query_type == 'recent_trades':
                        recent_time = datetime.now() - timedelta(hours=1)
                        await db.get_recent_trades(account_id, recent_time)
                    elif query_type == 'trade_volume':
                        await db.get_account_trade_volume(account_id)
                    elif query_type == 'time_range_activity':
                        start_time = datetime.now() - timedelta(hours=1)
                        end_time = datetime.now()
                        await db.count_trades_in_timerange(start_time, end_time)
                    
                    user_queries += 1
                    
                    
                except Exception:
                    pass
            
            user_time = time.time() - user_start
            return {
                'user_id': user_id,
                'queries_completed': user_queries,
                'total_time': user_time,
                'queries_per_second': user_queries / user_time if user_time > 0 else 0
            }
        
        start_time = time.time()
        concurrent_users = 10
        
        try:
            # Create concurrent user tasks
            user_tasks = [
                asyncio.create_task(antiabuse_user_simulation(user_id))
                for user_id in range(1, concurrent_users + 1)
            ]
            
            user_results = await asyncio.gather(*user_tasks, return_exceptions=True)
            
            successful_users = [r for r in user_results if isinstance(r, dict)]
            failed_users = len([r for r in user_results if isinstance(r, Exception)])
            
            total_queries = sum(r['queries_completed'] for r in successful_users)
        
        except Exception as e:
            await self.cleanup_test_data(db_name)
            return {**test_info, 'success': False, 'error': str(e)}
        finally:
            await self.cleanup_test_data(db_name)
        
        total_time = time.time() - start_time
        avg_query_time = sum(db.metrics['query_times']) / len(db.metrics['query_times']) if db.metrics.get('query_times') else 0
        avg_user_qps = (total_queries / total_time) / concurrent_users if concurrent_users > 0 else 0
        
        return {
            **test_info,
            'success': failed_users == 0,
            'setup_time': setup_time,
            'total_time': total_time,
            'concurrent_users': concurrent_users,
            'total_queries': total_queries,
            'successful_users': len(successful_users),
            'failed_users': failed_users,
            'avg_queries_per_second': total_queries / total_time,
            'avg_user_qps': avg_user_qps,
            'avg_query_latency_ms': avg_query_time * 1000,
            'test_accounts': len(test_accounts),
            'historical_data_size': len(historical_trades)
        }

    async def test_case_7_query_response_time(self, db_name: str) -> Dict[str, Any]:
        """Individual Query Response Time Validation"""
        test_info = {
            'test_id': 7,
            'name': 'Query Response Time Test',
            'description': 'Validates individual database query response times for anti-abuse operations',
            'category': 'query_performance',
            'importance': 'critical',
            'relevance': 'Ensures database can support fast anti-abuse queries'
        }
        
        db = self.databases[db_name]
        db.reset_metrics()
        
        setup_start = time.time()
        test_accounts = self.TEST_ACCOUNTS['small']
        realistic_dataset_size = 50000
        
        test_data = []
        for i in range(realistic_dataset_size):
            account_id = test_accounts[i % len(test_accounts)]
            timestamp = datetime.now() - timedelta(minutes=i % 1440)
            trade_data = self._generate_time_series_data(i + 1, account_id, timestamp)
            test_data.append(trade_data)
        
        await db.insert_trades_batch(test_data)
        setup_time = time.time() - setup_start
        db.reset_metrics()
        
        latency_tests = []
        acceptable_latency_ms = 100.0  # 100ms target for individual queries
        
        # Use standardized test accounts
        sample_accounts = test_accounts[:10]  # Use first 10 accounts for testing
        test_queries = [
            ('account_balance', sample_accounts[0]), ('account_balance', sample_accounts[1]), ('account_balance', sample_accounts[2]),
            ('trade_count', sample_accounts[0]), ('trade_count', sample_accounts[1]), ('trade_count', sample_accounts[2]), ('trade_count', sample_accounts[3]),
            ('recent_trades', sample_accounts[0]), ('recent_trades', sample_accounts[1]), ('recent_trades', sample_accounts[2]),
            ('trade_volume', sample_accounts[0]), ('trade_volume', sample_accounts[1]), ('trade_volume', sample_accounts[2]), ('trade_volume', sample_accounts[3]),
            ('time_range_1h', None), ('time_range_4h', None), ('time_range_24h', None),
            ('account_balance', sample_accounts[4]), ('trade_count', sample_accounts[4]), ('recent_trades', sample_accounts[4]), ('trade_volume', sample_accounts[4])
        ]
        
        start_time = time.time()
        
        try:
            for query_type, account_id in test_queries:
                query_start = time.perf_counter()
                
                try:
                    if query_type == 'account_balance':
                        await db.get_account_balance(account_id)
                    elif query_type == 'trade_count':
                        await db.count_account_trades(account_id)
                    elif query_type == 'recent_trades':
                        recent_time = datetime.now() - timedelta(hours=2)
                        await db.get_recent_trades(account_id, recent_time)
                    elif query_type == 'trade_volume':
                        await db.get_account_trade_volume(account_id)
                    elif query_type == 'time_range_1h':
                        start_dt = datetime.now() - timedelta(hours=1)
                        end_dt = datetime.now()
                        await db.count_trades_in_timerange(start_dt, end_dt)
                    elif query_type == 'time_range_4h':
                        start_dt = datetime.now() - timedelta(hours=4)
                        end_dt = datetime.now()
                        await db.count_trades_in_timerange(start_dt, end_dt)
                    elif query_type == 'time_range_24h':
                        start_dt = datetime.now() - timedelta(hours=24)
                        end_dt = datetime.now()
                        await db.count_trades_in_timerange(start_dt, end_dt)
                    
                    query_time = time.perf_counter() - query_start
                    
                    latency_ms = query_time * 1000
                    latency_tests.append({
                        'query_type': query_type,
                        'account_id': account_id,
                        'latency_seconds': query_time,
                        'meets_target': latency_ms <= acceptable_latency_ms,
                        'latency_ms': latency_ms
                    })
                    
                except Exception as e:
                    query_time = time.perf_counter() - query_start
                    latency_tests.append({
                        'query_type': query_type,
                        'account_id': account_id,
                        'latency_seconds': query_time,
                        'meets_target': False,
                        'latency_ms': query_time * 1000,
                        'error': str(e)
                    })
        
        except Exception as e:
            await self.cleanup_test_data(db_name)
            return {**test_info, 'success': False, 'error': str(e)}
        finally:
            await self.cleanup_test_data(db_name)
        
        total_time = time.time() - start_time
        
        total_queries = len(latency_tests)
        queries_meeting_target = len([t for t in latency_tests if t['meets_target']])
        target_compliance_rate = (queries_meeting_target / total_queries) * 100
        failed_queries = len([t for t in latency_tests if 'error' in t])
        
        # Calculate latency percentiles
        all_latencies = [t['latency_seconds'] for t in latency_tests]
        all_latencies.sort()
        
        if all_latencies:
            p50_latency = all_latencies[int(len(all_latencies) * 0.5)]
            p95_latency = all_latencies[int(len(all_latencies) * 0.95)]
            p99_latency = all_latencies[int(len(all_latencies) * 0.99)]
            max_latency = max(all_latencies)
            avg_latency = sum(all_latencies) / len(all_latencies)
        else:
            p50_latency = p95_latency = p99_latency = max_latency = avg_latency = 0
        
        worst_queries = sorted(latency_tests, key=lambda x: x['latency_seconds'], reverse=True)[:5]
        
        latency_by_type = {}
        for test in latency_tests:
            query_type = test['query_type']
            if query_type not in latency_by_type:
                latency_by_type[query_type] = []
            latency_by_type[query_type].append(test['latency_seconds'])
        
        # Success criteria: At least 75% of queries meet target and minimal failed queries
        success_criteria = target_compliance_rate >= 75.0 and failed_queries < (total_queries * 0.2)
        
        return {
            **test_info,
            'success': success_criteria,
            'setup_time': setup_time,
            'total_time': total_time,
            'total_queries': total_queries,
            'target_latency_ms': acceptable_latency_ms,
            'queries_meeting_target': queries_meeting_target,
            'failed_queries': failed_queries,
            'target_compliance_rate': target_compliance_rate,
            'p50_latency_ms': p50_latency * 1000,
            'p95_latency_ms': p95_latency * 1000,
            'p99_latency_ms': p99_latency * 1000,
            'max_latency_ms': max_latency * 1000,
            'avg_latency_ms': avg_latency * 1000,
            'worst_performing_queries': [
                {
                    'query': f"{q['query_type']} (account: {q.get('account_id', 'N/A')})",
                    'latency_ms': q['latency_ms'],
                    'meets_target': q['meets_target']
                } for q in worst_queries
            ],
            'latency_by_query_type': {
                qtype: {
                    'avg_ms': (sum(latencies) / len(latencies)) * 1000,
                    'max_ms': max(latencies) * 1000,
                    'min_ms': min(latencies) * 1000,
                    'count': len(latencies),
                    'target_violations': len([l for l in latencies if l * 1000 > acceptable_latency_ms])
                }
                for qtype, latencies in latency_by_type.items()
            },
            'test_dataset_size': realistic_dataset_size
        }

    async def test_case_8_sustained_load_stress(self, db_name: str) -> Dict[str, Any]:
        """Test 8: Progressive Load Stress Test"""
        test_info = {
            'test_id': 8,
            'name': 'Progressive Load Stress Test',
            'description': 'Tests system stability under progressively increasing load with performance degradation analysis',
            'category': 'stress_testing',
            'importance': 'high',
            'relevance': 'System breaking point and performance degradation under extreme load'
        }
        
        db = self.databases[db_name]
        db.reset_metrics()
        
        import psutil
        process = psutil.Process()
        
        # Stress test phases with increasing load
        stress_phases = [
            {'name': 'Baseline', 'duration_sec': 30, 'batch_size': 500, 'concurrent_ops': 1},
            {'name': 'Normal Load', 'duration_sec': 60, 'batch_size': 1000, 'concurrent_ops': 2},
            {'name': 'High Load', 'duration_sec': 90, 'batch_size': 2000, 'concurrent_ops': 4},
            {'name': 'Peak Load', 'duration_sec': 60, 'batch_size': 3000, 'concurrent_ops': 6},
            {'name': 'Extreme Load', 'duration_sec': 45, 'batch_size': 5000, 'concurrent_ops': 8}
        ]
        
        phase_results = []
        total_start_time = time.time()
        trade_id_counter = 0
        
        try:
            for phase_idx, phase in enumerate(stress_phases):
                phase_start_time = time.time()
                phase_end_time = phase_start_time + phase['duration_sec']
                
                phase_metrics = {
                    'phase_name': phase['name'],
                    'operations_completed': 0,
                    'operations_failed': 0,
                    'response_times': [],
                    'throughput_samples': [],
                    'memory_samples': [],
                    'cpu_samples': []
                }
                
                # Concurrent operations for this phase
                async def stress_operation(op_id: int):
                    """Single stress operation with mixed read/write"""
                    operation_start = time.perf_counter()
                    
                    try:
                        # Deterministic 70% writes, 30% reads pattern
                        if (op_id % 10) < 7:  # 70% writes (operations 0,1,2,3,4,5,6 are writes)
                            # Write operation - batch insert
                            batch_trades = []
                            for _ in range(phase['batch_size']):
                                nonlocal trade_id_counter
                                trade_id_counter += 1
                                account_id = self.TEST_ACCOUNTS['small'][(trade_id_counter % len(self.TEST_ACCOUNTS['small']))]
                                trade_data = self._generate_time_series_data(
                                    trade_id_counter,
                                    account_id,
                                    datetime.now()
                                )
                                batch_trades.append(trade_data)
                            
                            await db.insert_trades_batch(batch_trades)
                            phase_metrics['operations_completed'] += len(batch_trades)
                        else:
                            # Read operation - query (operations 7,8,9 are reads)
                            account_id = self.TEST_ACCOUNTS['small'][(op_id % len(self.TEST_ACCOUNTS['small']))]  # Deterministic account selection
                            await db.count_account_trades(account_id)
                            phase_metrics['operations_completed'] += 1
                            
                        operation_time = time.perf_counter() - operation_start
                        phase_metrics['response_times'].append(operation_time)
                        
                    except Exception as e:
                        phase_metrics['operations_failed'] += 1
                        pass
                
                # Run phase with concurrent operations
                current_time = phase_start_time
                operation_counter = 0
                
                while current_time < phase_end_time:
                    # Create concurrent tasks for this interval
                    tasks = []
                    for i in range(phase['concurrent_ops']):
                        task = asyncio.create_task(stress_operation(operation_counter + i))
                        tasks.append(task)
                    
                    interval_start = time.perf_counter()
                    
                    # Wait for operations to complete
                    await asyncio.gather(*tasks, return_exceptions=True)
                    
                    interval_time = time.perf_counter() - interval_start
                    operation_counter += phase['concurrent_ops']
                    
                    # Calculate throughput for this interval
                    throughput = phase['concurrent_ops'] / interval_time if interval_time > 0 else 0
                    phase_metrics['throughput_samples'].append(throughput)
                    
                    # Sample system metrics
                    memory_mb = process.memory_info().rss / 1024 / 1024
                    cpu_percent = process.cpu_percent()
                    phase_metrics['memory_samples'].append(memory_mb)
                    phase_metrics['cpu_samples'].append(cpu_percent)
                    
                    current_time = time.time()
                    
                
                phase_duration = time.time() - phase_start_time
                
                # Calculate phase statistics
                total_operations = phase_metrics['operations_completed'] + phase_metrics['operations_failed']
                error_rate = (phase_metrics['operations_failed'] / total_operations * 100) if total_operations > 0 else 0
                
                response_times = phase_metrics['response_times']
                avg_response_time = sum(response_times) / len(response_times) if response_times else 0
                p95_response_time = sorted(response_times)[int(len(response_times) * 0.95)] if response_times else 0
                
                throughput_samples = phase_metrics['throughput_samples']
                avg_throughput = sum(throughput_samples) / len(throughput_samples) if throughput_samples else 0
                
                memory_samples = phase_metrics['memory_samples']
                avg_memory = sum(memory_samples) / len(memory_samples) if memory_samples else 0
                peak_memory = max(memory_samples) if memory_samples else 0
                
                cpu_samples = phase_metrics['cpu_samples']
                avg_cpu = sum(cpu_samples) / len(cpu_samples) if cpu_samples else 0
                
                phase_result = {
                    'phase_name': phase['name'],
                    'phase_index': phase_idx + 1,
                    'duration_sec': phase_duration,
                    'target_concurrent_ops': phase['concurrent_ops'],
                    'target_batch_size': phase['batch_size'],
                    'operations_completed': phase_metrics['operations_completed'],
                    'operations_failed': phase_metrics['operations_failed'],
                    'error_rate_percent': error_rate,
                    'avg_response_time_ms': avg_response_time * 1000,
                    'p95_response_time_ms': p95_response_time * 1000,
                    'avg_throughput_ops_sec': avg_throughput,
                    'avg_memory_mb': avg_memory,
                    'peak_memory_mb': peak_memory,
                    'avg_cpu_percent': avg_cpu
                }
                
                phase_results.append(phase_result)
                
        
        except Exception as e:
            return {**test_info, 'success': False, 'error': str(e)}
        
        total_time = time.time() - total_start_time
        
        # Overall analysis
        total_operations = sum(p['operations_completed'] for p in phase_results)
        total_failures = sum(p['operations_failed'] for p in phase_results)
        overall_error_rate = (total_failures / (total_operations + total_failures) * 100) if (total_operations + total_failures) > 0 else 0
        
        # Performance degradation analysis
        baseline_throughput = phase_results[0]['avg_throughput_ops_sec'] if phase_results else 0
        final_throughput = phase_results[-1]['avg_throughput_ops_sec'] if phase_results else 0
        throughput_degradation = ((baseline_throughput - final_throughput) / baseline_throughput * 100) if baseline_throughput > 0 else 0
        
        # System stability analysis
        baseline_response = phase_results[0]['avg_response_time_ms'] if phase_results else 0
        final_response = phase_results[-1]['avg_response_time_ms'] if phase_results else 0
        response_degradation = ((final_response - baseline_response) / baseline_response * 100) if baseline_response > 0 else 0
        
        # Find breaking point (first phase with >10% error rate)
        breaking_point_phase = None
        for phase in phase_results:
            if phase['error_rate_percent'] > 10:
                breaking_point_phase = phase['phase_name']
                break
        
        # Success criteria: system should handle at least "High Load" phase with <5% error rate
        high_load_success = False
        for phase in phase_results:
            if phase['phase_name'] == 'High Load' and phase['error_rate_percent'] < 5:
                high_load_success = True
                break
        
        return {
            **test_info,
            'success': high_load_success and overall_error_rate < 10,
            'total_time': total_time,
            'total_operations': total_operations,
            'total_failures': total_failures,
            'overall_error_rate_percent': overall_error_rate,
            'throughput_degradation_percent': throughput_degradation,
            'response_time_degradation_percent': response_degradation,
            'breaking_point_phase': breaking_point_phase or 'None - System stable',
            'phases_completed': len(phase_results),
            'phase_results': phase_results,
            'stress_test_summary': {
                'max_concurrent_ops_stable': max([p['target_concurrent_ops'] for p in phase_results if p['error_rate_percent'] < 5], default=0),
                'max_batch_size_stable': max([p['target_batch_size'] for p in phase_results if p['error_rate_percent'] < 5], default=0),
                'peak_stable_throughput': max([p['avg_throughput_ops_sec'] for p in phase_results if p['error_rate_percent'] < 5], default=0),
                'system_resilience_score': 100 - min(100, overall_error_rate + throughput_degradation / 2)
            }
        }
    
    async def test_case_9_resource_usage_monitoring(self, db_name: str) -> Dict[str, Any]:
        """Test 9: Resource Usage and Connection Pool Efficiency"""
        test_info = {
            'test_id': 9,
            'name': 'Resource Usage Monitoring',
            'description': 'Tests database connection efficiency and resource management under varied workloads',
            'category': 'resource_monitoring',
            'importance': 'medium',
            'relevance': 'Connection pool optimization and resource leak detection'
        }
        
        db = self.databases[db_name]
        db.reset_metrics()
        
        import psutil
        process = psutil.Process()
        
        # Evaluate database resource utilization patterns
        workload_patterns = [
            {'name': 'Small Frequent Operations', 'batches': 50, 'batch_size': 100, 'queries_between': 5},
            {'name': 'Medium Batch Operations', 'batches': 20, 'batch_size': 500, 'queries_between': 10},
            {'name': 'Large Batch Operations', 'batches': 10, 'batch_size': 1000, 'queries_between': 20},
            {'name': 'Mixed Workload Pattern', 'batches': 30, 'batch_size': 300, 'queries_between': 15}
        ]
        
        pattern_results = []
        start_time = time.time()
        trade_id_counter = 0
        
        try:
            for _, pattern in enumerate(workload_patterns):
                pattern_start = time.time()
                
                # Initial resource measurement
                initial_memory = process.memory_info().rss / 1024 / 1024
                initial_cpu = process.cpu_percent()
                
                resource_samples = {
                    'memory_mb': [initial_memory],
                    'cpu_percent': [initial_cpu],
                    'operation_times': [],
                    'query_times': []
                }
                
                # Execute workload pattern
                for batch_idx in range(pattern['batches']):
                    # Batch insert operation
                    batch_start = time.perf_counter()
                    batch_trades = []
                    
                    for i in range(pattern['batch_size']):
                        trade_id_counter += 1
                        account_id = self.TEST_ACCOUNTS['small'][(trade_id_counter % len(self.TEST_ACCOUNTS['small']))]  # Deterministic accounts
                        timestamp = datetime.now() - timedelta(minutes=(batch_idx * 10 + i) % 1440)
                        trade_data = self._generate_time_series_data(trade_id_counter, account_id, timestamp)
                        batch_trades.append(trade_data)
                    
                    await db.insert_trades_batch(batch_trades)
                    batch_time = time.perf_counter() - batch_start
                    resource_samples['operation_times'].append(batch_time)
                    
                    # Query operations between batches
                    for query_idx in range(pattern['queries_between']):
                        query_start = time.perf_counter()
                        account_id = self.TEST_ACCOUNTS['small'][((batch_idx * pattern['queries_between'] + query_idx) % len(self.TEST_ACCOUNTS['small']))]
                        
                        # Deterministic query type selection
                        query_type = ['balance', 'count', 'volume', 'recent'][query_idx % 4]
                        
                        if query_type == 'balance':
                            await db.get_account_balance(account_id)
                        elif query_type == 'count':
                            await db.count_account_trades(account_id)
                        elif query_type == 'volume':
                            await db.get_account_trade_volume(account_id)
                        else:  # recent
                            recent_time = datetime.now() - timedelta(hours=1)
                            await db.get_recent_trades(account_id, recent_time)
                        
                        query_time = time.perf_counter() - query_start
                        resource_samples['query_times'].append(query_time)
                    
                    # Sample resources every few batches
                    if batch_idx % 5 == 0:
                        memory_mb = process.memory_info().rss / 1024 / 1024
                        cpu_percent = process.cpu_percent()
                        resource_samples['memory_mb'].append(memory_mb)
                        resource_samples['cpu_percent'].append(cpu_percent)
                
                pattern_duration = time.time() - pattern_start
                
                # Calculate pattern statistics
                final_memory = process.memory_info().rss / 1024 / 1024
                memory_usage = final_memory - initial_memory
                
                avg_operation_time = sum(resource_samples['operation_times']) / len(resource_samples['operation_times'])
                avg_query_time = sum(resource_samples['query_times']) / len(resource_samples['query_times'])
                
                peak_memory = max(resource_samples['memory_mb'])
                avg_cpu = sum(resource_samples['cpu_percent']) / len(resource_samples['cpu_percent'])
                
                total_operations = pattern['batches'] * pattern['batch_size'] + pattern['batches'] * pattern['queries_between']
                operations_per_second = total_operations / pattern_duration
                
                pattern_result = {
                    'pattern_name': pattern['name'],
                    'duration_sec': pattern_duration,
                    'total_operations': total_operations,
                    'operations_per_second': operations_per_second,
                    'avg_operation_time_ms': avg_operation_time * 1000,
                    'avg_query_time_ms': avg_query_time * 1000,
                    'memory_usage_mb': memory_usage,
                    'peak_memory_mb': peak_memory,
                    'avg_cpu_percent': avg_cpu,
                    'resource_efficiency_score': operations_per_second / max(1, memory_usage),  # ops per MB
                    'batches_completed': pattern['batches'],
                    'queries_completed': pattern['batches'] * pattern['queries_between']
                }
                
                pattern_results.append(pattern_result)
                
        
        except Exception as e:
            await self.cleanup_test_data(db_name)
            return {**test_info, 'success': False, 'error': str(e)}
        finally:
            await self.cleanup_test_data(db_name)
        
        total_time = time.time() - start_time
        
        # Overall resource analysis
        total_operations = sum(p['total_operations'] for p in pattern_results)
        avg_efficiency = sum(p['resource_efficiency_score'] for p in pattern_results) / len(pattern_results)
        peak_memory_usage = max(p['peak_memory_mb'] for p in pattern_results)
        
        # Find most efficient pattern
        most_efficient_pattern = max(pattern_results, key=lambda x: x['resource_efficiency_score'])
        
        # Connection stability assessment (based on consistent performance)
        operation_times = []
        query_times = []
        for p in pattern_results:
            operation_times.append(p['avg_operation_time_ms'])
            query_times.append(p['avg_query_time_ms'])
        
        # Calculate coefficient of variation (stability metric)
        max_op_time = max(operation_times) if operation_times else 1
        max_query_time = max(query_times) if query_times else 1
        operation_stability = (max_op_time - min(operation_times)) / max(max_op_time, 1) * 100 if operation_times else 0
        query_stability = (max_query_time - min(query_times)) / max(max_query_time, 1) * 100 if query_times else 0
        
        # Success criteria: stable performance across patterns and reasonable resource usage
        stability_good = operation_stability < 80 and query_stability < 80  # Less than 80% variation (more realistic)
        memory_reasonable = peak_memory_usage < 500  # Less than 500MB peak usage
        
        return {
            **test_info,
            'success': stability_good and memory_reasonable,
            'total_time': total_time,
            'total_operations': total_operations,
            'patterns_tested': len(pattern_results),
            'avg_resource_efficiency': avg_efficiency,
            'peak_memory_usage_mb': peak_memory_usage,
            'operation_stability_percent': operation_stability,
            'query_stability_percent': query_stability,
            'most_efficient_pattern': {
                'name': most_efficient_pattern['pattern_name'],
                'efficiency_score': most_efficient_pattern['resource_efficiency_score'],
                'ops_per_second': most_efficient_pattern['operations_per_second']
            },
            'pattern_results': pattern_results,
            'resource_management_assessment': {
                'connection_stability': 'Good' if stability_good else 'Needs Improvement',
                'memory_efficiency': 'Good' if memory_reasonable else 'High Usage',
                'overall_score': (100 - operation_stability - query_stability / 2) if stability_good and memory_reasonable else 50
            }
        }

    async def test_case_10_bulk_update_performance(self, db_name: str) -> Dict[str, Any]:
        """Bulk Update Operations Performance Test"""
        test_info = {
            'test_id': 10,
            'name': 'Bulk Update Performance Test',
            'description': 'Tests database performance for bulk update operations with various batch sizes',
            'category': 'update_performance',
            'importance': 'high',
            'relevance': 'Account balance updates are critical for anti-abuse risk calculations'
        }
        
        db = self.databases[db_name]
        db.reset_metrics()
        
        try:
            start_time = time.time()
            
            # Setup: Create test accounts with initial balances
            setup_start = time.time()
            test_accounts = self.TEST_ACCOUNTS['bulk_update']  # 500 accounts for bulk update testing
            
            # Insert initial account data
            initial_trades = []
            for i, account_id in enumerate(test_accounts):
                trade_data = self._generate_time_series_data(
                    trade_id=100000 + i,
                    account_id=account_id,
                    timestamp=datetime.now() - timedelta(minutes=i % 60)
                )
                initial_trades.append(trade_data)
            
            # Batch insert initial data
            if initial_trades:
                await db.insert_trades_batch(initial_trades)
            
            setup_time = time.time() - setup_start
            
            # Test different bulk update patterns (optimized for disk space)
            update_patterns = [
                {'name': 'Small Batch High Frequency', 'batch_size': 10, 'batches': 300, 'update_frequency': 0.001},
                {'name': 'Medium Batch Updates', 'batch_size': 50, 'batches': 60, 'update_frequency': 0.005},
                {'name': 'Large Batch Updates', 'batch_size': 200, 'batches': 15, 'update_frequency': 0.01},
                {'name': 'Ultra Large Batch Updates', 'batch_size': 500, 'batches': 6, 'update_frequency': 0.02},
                {'name': 'Individual High Volume', 'batch_size': 1, 'batches': 1000, 'update_frequency': 0.0001},
                {'name': 'Maximum Stress Batch', 'batch_size': 1000, 'batches': 3, 'update_frequency': 0.0}
            ]
            
            pattern_results = []
            total_updates = 0
            
            for pattern in update_patterns:
                pattern_start = time.time()
                pattern_updates = 0
                update_times = []
                
                for batch_idx in range(pattern['batches']):
                    # Select accounts for this batch
                    start_idx = (batch_idx * pattern['batch_size']) % len(test_accounts)
                    end_idx = min(start_idx + pattern['batch_size'], len(test_accounts))
                    batch_accounts = test_accounts[start_idx:end_idx]
                    
                    if not batch_accounts:  # Handle wraparound
                        batch_accounts = test_accounts[:pattern['batch_size']]
                    
                    # Perform updates
                    for account_id in batch_accounts:
                        update_start = time.perf_counter()
                        # Simulate balance update (increase by deterministic amount)
                        new_balance = 1000.0 + (account_id % 1000) + (batch_idx * 10.5)
                        await db.update_account_balance(account_id, new_balance)
                        update_time = time.perf_counter() - update_start
                        update_times.append(update_time)
                        pattern_updates += 1
                    
                    # Small delay between batches for realistic simulation
                    await asyncio.sleep(pattern['update_frequency'])
                    
                pattern_duration = time.time() - pattern_start
                
                # Calculate pattern statistics
                if update_times:
                    avg_update_time = sum(update_times) / len(update_times)
                    sorted_times = sorted(update_times)
                    p50_update_time = sorted_times[int(len(sorted_times) * 0.5)]
                    p95_update_time = sorted_times[int(len(sorted_times) * 0.95)]
                    max_update_time = max(update_times)
                else:
                    avg_update_time = p50_update_time = p95_update_time = max_update_time = 0
                    
                updates_per_second = pattern_updates / pattern_duration if pattern_duration > 0 else 0
                
                pattern_result = {
                    'pattern_name': pattern['name'],
                    'duration_sec': pattern_duration,
                    'total_updates': pattern_updates,
                    'updates_per_second': updates_per_second,
                    'avg_update_time_ms': avg_update_time * 1000,
                    'p50_update_time_ms': p50_update_time * 1000,
                    'p95_update_time_ms': p95_update_time * 1000,
                    'max_update_time_ms': max_update_time * 1000,
                    'batch_size': pattern['batch_size'],
                    'batches_completed': pattern['batches']
                }
                
                pattern_results.append(pattern_result)
                total_updates += pattern_updates
            
            total_time = time.time() - start_time
            
            # Find best performing pattern
            best_pattern = max(pattern_results, key=lambda x: x['updates_per_second'])
            
            # Overall performance metrics
            overall_updates_per_second = total_updates / total_time if total_time > 0 else 0
            
            # Database-specific metrics
            db_update_times = db.metrics.get('update_times', [])
            avg_db_update_time = sum(db_update_times) / len(db_update_times) if db_update_times else 0
            
            # Performance thresholds for success
            performance_good = overall_updates_per_second >= 100  # At least 100 updates/sec
            consistency_good = all(p['updates_per_second'] > 0 for p in pattern_results)
            
            return {
                **test_info,
                'success': performance_good and consistency_good,
                'setup_time': setup_time,
                'total_time': total_time,
                'total_updates': total_updates,
                'overall_updates_per_second': overall_updates_per_second,
                'avg_database_update_time_ms': avg_db_update_time * 1000,
                'test_accounts': len(test_accounts),
                'patterns_tested': len(pattern_results),
                'best_performing_pattern': {
                    'name': best_pattern['pattern_name'],
                    'updates_per_second': best_pattern['updates_per_second'],
                    'avg_update_time_ms': best_pattern['avg_update_time_ms']
                },
                'pattern_results': pattern_results,
                'performance_rating': 'EXCELLENT' if overall_updates_per_second >= 500 else 
                                   'GOOD' if overall_updates_per_second >= 200 else
                                   'AVERAGE' if overall_updates_per_second >= 100 else 'POOR'
            }
            
        except Exception as e:
            return {**test_info, 'success': False, 'error': str(e)}

    def get_all_test_methods(self):
        """Get all test methods dynamically"""
        test_methods = []
        
        for attr_name in dir(self):
            if attr_name.startswith('test_case_') and callable(getattr(self, attr_name)):
                test_number = int(attr_name.split('_')[2])
                method = getattr(self, attr_name)
                test_methods.append((test_number, method))
        
        return sorted(test_methods, key=lambda x: x[0])