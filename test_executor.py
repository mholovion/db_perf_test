"""
Test Execution Engine - handles running tests and collecting results
Separated execution logic from test definitions
"""
import asyncio
import argparse
import logging
from datetime import datetime
from typing import List, Dict, Any
from tabulate import tabulate

from all_tests import AllTests
from database_interfaces import QuestDBInterface, ClickHouseInterface
from csv_exporter import EnhancedCSVExporter

# Ensure directories exist with cross-platform paths
import os
from pathlib import Path

# Create directories using pathlib for cross-platform compatibility  
logs_dir = Path('logs')
results_dir = Path('results')
logs_dir.mkdir(exist_ok=True)
results_dir.mkdir(exist_ok=True)

# Configure logging with cross-platform path
log_file = logs_dir / 'test_suite.log'
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(log_file, mode='w'),  # Overwrite log file
        logging.StreamHandler()
    ]
)
logger = logging.getLogger(__name__)

class TestExecutor:
    """Main test execution engine"""
    
    def __init__(self):
        self.databases = {
            'QuestDB': QuestDBInterface(),
            'ClickHouse': ClickHouseInterface(),
        }
        self.all_tests = AllTests(self.databases, None)
        self.results = {}
    
    
    async def run_tests_for_database(self, db_name: str, test_cases: List[int] = None):
        """Run tests for a specific database"""
        logger.info(f"Starting tests for {db_name}")
        
        try:
            # Connect to database
            await self.databases[db_name].connect()
            await self.databases[db_name].create_schema()
            
            # Get all available tests
            all_test_methods = self.all_tests.get_all_test_methods()
            
            # Filter tests if specific test cases requested
            if test_cases:
                test_methods = [(num, method) for num, method in all_test_methods if num in test_cases]
            else:
                test_methods = all_test_methods
            
            self.results[db_name] = {}
            
            # Run each test
            for test_number, test_method in test_methods:
                # Get clean test name from docstring, then format with test number
                base_name = test_method.__doc__.split('\\n')[0].strip() if test_method.__doc__ else f"Unknown Test"
                test_name = f"Test {test_number}: {base_name}"
                logger.info(f"Running {test_name}")
                
                try:
                    result = await test_method(db_name)
                    self.results[db_name][test_name] = result
                    
                    if result.get('success', False):
                        logger.info(f"✓ {test_name} completed successfully")
                    else:
                        logger.error(f"✗ {test_name} failed: {result.get('error', 'Unknown error')}")
                        
                except Exception as e:
                    logger.error(f"✗ {test_name} failed with exception: {e}")
                    self.results[db_name][test_name] = {
                        'success': False, 
                        'error': str(e),
                        'test_id': test_number,
                        'name': test_name
                    }
            
            # Disconnect from database
            await self.databases[db_name].disconnect()
            logger.info(f"Completed tests for {db_name}")
            
        except Exception as e:
            logger.error(f"Failed to test {db_name}: {e}")
            self.results[db_name] = {'error': f"Failed to connect or setup: {e}"}
    
    async def run_all_tests(self, db_names: List[str] = None, test_cases: List[int] = None):
        """Run all tests for specified databases"""
        if db_names is None:
            db_names = ['QuestDB', 'ClickHouse']  # Default to available DBs
        
        logger.info(f"Starting comprehensive test suite")
        logger.info(f"Databases: {', '.join(db_names)}")
        logger.info(f"Test Cases: {'All' if not test_cases else f'Cases {test_cases}'}")
        
        # Run tests for each database
        for db_name in db_names:
            await self.run_tests_for_database(db_name, test_cases)
        
        # Display and export results
        self.display_results()
        await self.export_results()
    
    def display_results(self):
        """Display test results in formatted tables"""
        print(f"\\n{'='*100}")
        print(f"TEST RESULTS SUMMARY")
        print(f"{'='*100}")
        
        # Create summary table
        summary_data = []
        
        # Get all unique test names from results
        all_test_names = set()
        for db_name, db_results in self.results.items():
            if 'error' not in db_results:
                all_test_names.update(db_results.keys())
        
        all_test_names = sorted(list(all_test_names))
        
        for db_name in self.results.keys():
            if 'error' in self.results[db_name]:
                row = [db_name] + ['ERROR'] * len(all_test_names)
                summary_data.append(row)
                continue
            
            row = [db_name]
            for test_name in all_test_names:
                if test_name in self.results[db_name]:
                    result = self.results[db_name][test_name]
                    if result.get('success', False):
                        row.append('✓ PASS')
                    else:
                        row.append('✗ FAIL')
                else:
                    row.append('N/A')
            summary_data.append(row)
        
        # Truncate test names for display
        display_headers = ['Database'] + [name[:20] + '...' if len(name) > 20 else name for name in all_test_names]
        print(tabulate(summary_data, headers=display_headers, tablefmt='grid'))
        
        # Display detailed performance metrics
        self.display_performance_metrics()
        
        # Display summary statistics
        self.display_summary_statistics()
    
    def display_performance_metrics(self):
        """Display key performance metrics"""
        print(f"\\nKEY PERFORMANCE METRICS")
        print(f"{'='*80}")
        
        for db_name, db_results in self.results.items():
            if 'error' in db_results:
                continue
            
            print(f"\\n{db_name}:")
            
            for test_name, result in db_results.items():
                if not isinstance(result, dict) or not result.get('success', False):
                    continue
                
                # Extract and display key metrics based on test type
                if 'trades_per_second' in result:
                    print(f"  {test_name}: {result['trades_per_second']:.1f} trades/sec")
                elif 'throughput' in result:
                    print(f"  {test_name}: {result['throughput']:.1f} ops/sec")
                elif 'operations_per_second' in result:
                    print(f"  {test_name}: {result['operations_per_second']:.1f} ops/sec")
                elif 'avg_risk_score_time' in result:
                    print(f"  {test_name}: {result['avg_risk_score_time']*1000:.1f}ms avg")
                elif 'user_ops_per_second' in result:
                    print(f"  {test_name}: {result['user_ops_per_second']:.1f} user-ops/sec")
                elif 'winner' in result:
                    print(f"  {test_name}: {result['winner']} pattern wins")
                elif 'memory_growth_mb' in result:
                    print(f"  {test_name}: {result['memory_growth_mb']:.1f}MB growth")
    
    def display_summary_statistics(self):
        """Display overall summary statistics"""
        print(f"\\nSUMMARY STATISTICS")
        print(f"{'='*80}")
        
        for db_name, db_results in self.results.items():
            if 'error' in db_results:
                continue
            
            successful_tests = sum(1 for result in db_results.values() 
                                 if isinstance(result, dict) and result.get('success', False))
            total_tests = len([r for r in db_results.values() if isinstance(r, dict)])
            success_rate = (successful_tests / total_tests) * 100 if total_tests > 0 else 0
            
            print(f"{db_name}:")
            print(f"  Tests Passed: {successful_tests}/{total_tests}")
            print(f"  Success Rate: {success_rate:.1f}%")
            
            # Reliability assessment
            if success_rate >= 90:
                print(f"  Assessment: Excellent reliability")
            elif success_rate >= 75:
                print(f"  Assessment: Good reliability")
            else:
                print(f"  Assessment: Needs improvement")
    
    async def export_results(self):
        """Export results to essential formats only"""
        import json
        
        logger.info("Exporting results...")
        
        # Export to JSON
        results_file = results_dir / 'test_results.json'
        with open(results_file, 'w') as f:
            json.dump(self.results, f, indent=2, default=str)
        
        # Export enhanced CSV with performance analysis
        csv_exporter = EnhancedCSVExporter(self.results)
        csv_file = results_dir / 'performance_analysis.csv'
        csv_exporter.export_enhanced_csv(str(csv_file))
        
        logger.info("Results exported to:")
        logger.info(f"  - {results_file} (raw test data)")
        logger.info(f"  - {csv_file} (detailed analysis)")
        logger.info(f"  - {log_file} (execution log)")

async def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(description='Anti-Abuse Database Performance Test Suite')
    parser.add_argument('--databases', '-d', nargs='+', 
                       choices=['QuestDB', 'ClickHouse'],
                       help='Databases to test (default: QuestDB, ClickHouse)')
    parser.add_argument('--test-cases', '-t', nargs='+', type=int,
                       choices=range(1, 12),
                       help='Specific test cases to run (default: all 1-11)')
    
    args = parser.parse_args()
    
    executor = TestExecutor()
    
    databases_to_test = args.databases if args.databases else ['QuestDB', 'ClickHouse']
    test_cases_to_run = args.test_cases if args.test_cases else None
    
    print(f"Anti-Abuse System Database Performance Test Suite")
    print(f"================================================")
    print(f"Databases: {', '.join(databases_to_test)}")
    print(f"Test Cases: {'All' if not test_cases_to_run else f'Cases {test_cases_to_run}'}")
    print(f"Started: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print(f"================================================")
    
    await executor.run_all_tests(databases_to_test, test_cases_to_run)

if __name__ == "__main__":
    asyncio.run(main())