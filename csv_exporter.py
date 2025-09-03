import csv
from datetime import datetime
from typing import Dict, Any

class EnhancedCSVExporter:
    """Enhanced CSV exporter with performance ratings and clear indicators - dynamically extracts test info"""
    
    def __init__(self, results: Dict[str, Any]):
        self.results = results
        self.test_configs = self._extract_test_configs_from_results()
    
    def _extract_test_configs_from_results(self) -> Dict[int, Dict[str, Any]]:
        """Dynamically extract test configurations from test results"""
        test_configs = {}
        
        # Common metric mappings for determining best metric and units
        metric_mappings = {
            'trades_per_second': {'unit': ' trades/sec', 'higher_better': True, 'threshold': 1000, 'precision': 2},
            'throughput': {'unit': ' ops/sec', 'higher_better': True, 'threshold': 3000, 'precision': 1},
            'operations_per_second': {'unit': ' ops/sec', 'higher_better': True, 'threshold': 500, 'precision': 1},
            'read_throughput': {'unit': ' ops/sec', 'higher_better': True, 'threshold': 100, 'precision': 1},
            'user_ops_per_second': {'unit': ' ops/sec', 'higher_better': True, 'threshold': 50, 'precision': 2},
            'avg_risk_score_time': {'unit': 'ms', 'higher_better': False, 'multiply': 1000, 'threshold': 10, 'precision': 3},
            'detection_throughput': {'unit': ' det/sec', 'higher_better': True, 'threshold': 5, 'precision': 2},
            'breach_detection_rate': {'unit': ' br/sec', 'higher_better': True, 'threshold': 1, 'precision': 3},
            'total_queries_per_second': {'unit': ' q/sec', 'higher_better': True, 'threshold': 10, 'precision': 2},
            'retention_queries_per_second': {'unit': ' q/sec', 'higher_better': True, 'threshold': 5, 'precision': 2},
            'memory_growth_mb': {'unit': 'MB', 'higher_better': False, 'threshold': 100, 'precision': 2},
            # New metrics for Scale and Stress Testing
            'scale_efficiency': {'unit': '', 'higher_better': True, 'threshold': 0.1, 'precision': 4},
            'query_throughput': {'unit': ' q/sec', 'higher_better': True, 'threshold': 10, 'precision': 2},
            'success_rate': {'unit': '%', 'higher_better': True, 'threshold': 90, 'precision': 2},
            'latency_compliance_rate': {'unit': '%', 'higher_better': True, 'threshold': 95, 'precision': 2},
            'stress_resilience': {'unit': '', 'higher_better': True, 'threshold': 0.85, 'precision': 4},
            'user_concurrency_efficiency': {'unit': ' ops/user/min', 'higher_better': True, 'threshold': 5, 'precision': 2},
            'avg_retention_query_time_ms': {'unit': 'ms', 'higher_better': False, 'threshold': 100, 'precision': 3},
            # Test case 7 metrics
            'avg_latency_ms': {'unit': 'ms', 'higher_better': False, 'threshold': 100, 'precision': 4},
            'p50_latency_ms': {'unit': 'ms', 'higher_better': False, 'threshold': 100, 'precision': 4},
            'p95_latency_ms': {'unit': 'ms', 'higher_better': False, 'threshold': 100, 'precision': 4},
            'p99_latency_ms': {'unit': 'ms', 'higher_better': False, 'threshold': 100, 'precision': 4},
            'max_latency_ms': {'unit': 'ms', 'higher_better': False, 'threshold': 100, 'precision': 4},
            'target_compliance_rate': {'unit': '%', 'higher_better': True, 'threshold': 95, 'precision': 2},
            'total_time': {'unit': 'sec', 'higher_better': False, 'threshold': 1.0, 'precision': 6},
            # Test case 9 metrics - resource monitoring
            'avg_resource_efficiency': {'unit': ' ops/MB', 'higher_better': True, 'threshold': 1000, 'precision': 1},
            'peak_memory_usage_mb': {'unit': 'MB', 'higher_better': False, 'threshold': 100, 'precision': 2},
            'operation_stability_percent': {'unit': '%', 'higher_better': False, 'threshold': 50, 'precision': 2},
            # Test case 10 metrics - bulk updates
            'overall_updates_per_second': {'unit': ' upd/sec', 'higher_better': True, 'threshold': 100, 'precision': 2},
            'avg_database_update_time_ms': {'unit': 'ms', 'higher_better': False, 'threshold': 10, 'precision': 4}
        }
        
        # Extract test info from all database results
        for db_name, db_results in self.results.items():
            if 'error' in db_results:
                continue
                
            for test_name, result in db_results.items():
                if isinstance(result, dict) and 'test_id' in result:
                    test_id = result['test_id']
                    
                    # Skip if we already processed this test
                    if test_id in test_configs:
                        continue
                    
                    # Extract test metadata
                    test_config = {
                        'name': result.get('name', f'Test {test_id}'),
                        'description': result.get('description', 'No description'),
                        'category': result.get('category', 'unknown'),
                        'importance': result.get('importance', 'medium'),
                        'anti_abuse_relevance': result.get('anti_abuse_relevance', 'general')
                    }
                    
                    # Determine primary metric and its properties
                    primary_metric = self._determine_primary_metric(result)
                    if primary_metric and primary_metric in metric_mappings:
                        test_config.update(metric_mappings[primary_metric])
                        test_config['metric'] = primary_metric
                    else:
                        # Fallback for unknown metrics
                        test_config.update({
                            'metric': primary_metric or 'success',
                            'unit': '',
                            'higher_better': True,
                            'threshold': 0,
                            'precision': 3
                        })
                    
                    test_configs[test_id] = test_config
        
        return test_configs
    
    def _determine_primary_metric(self, result: Dict[str, Any]) -> str:
        """Determine the primary performance metric for a test result"""
        if not isinstance(result, dict):
            return 'success'
        
        # Priority order of metrics to look for
        priority_metrics = [
            'trades_per_second',
            'throughput', 
            'operations_per_second',
            'read_throughput',
            'user_ops_per_second',
            'detection_throughput',
            'breach_detection_rate',
            'avg_risk_score_time',
            'total_queries_per_second',
            'retention_queries_per_second',
            'memory_growth_mb',
            # New metrics for Scale and Stress Testing
            'scale_efficiency',
            'query_throughput', 
            'success_rate',
            'latency_compliance_rate',
            'stress_resilience',
            'user_concurrency_efficiency',
            'avg_retention_query_time_ms',
            # Test case 7 metrics - individual query latency
            'avg_latency_ms',
            'p50_latency_ms',
            'p95_latency_ms',
            'p99_latency_ms',
            'max_latency_ms',
            'target_compliance_rate',
            # Test case 9 metrics - resource monitoring (high priority)
            'avg_resource_efficiency',
            'peak_memory_usage_mb', 
            'operation_stability_percent',
            # Test case 10 metrics - bulk updates  
            'overall_updates_per_second',
            'avg_database_update_time_ms',
            # Fallback metric
            'total_time'
        ]
        
        # Find the highest priority metric that exists in results
        for metric in priority_metrics:
            if metric in result and isinstance(result[metric], (int, float)):
                return metric
        
        # Fallback to any numeric metric
        for key, value in result.items():
            if isinstance(value, (int, float)) and key not in ['test_id', 'success', 'total_time']:
                return key
        
        return 'success'
        
    def collect_performance_data(self):
        """Collect all performance data for comparison"""
        performance_data = {}
        
        for test_id, config in self.test_configs.items():
            performance_data[test_id] = {}
            for db_name, db_results in self.results.items():
                if 'error' in db_results:
                    continue
                for test_name, result in db_results.items():
                    if isinstance(result, dict) and result.get('test_id') == test_id:
                        if result.get('success', False) and config['metric'] in result:
                            value = result[config['metric']]
                            if config.get('multiply'):
                                value *= config['multiply']
                            performance_data[test_id][db_name] = value
        
        return performance_data
    
    def get_performance_rating(self, test_id: int, db_name: str, value: float, performance_data: dict):
        """Generate performance rating for a value"""
        config = self.test_configs[test_id]
        
        if config['metric'] == 'winner':
            return str(value), "STRATEGY_RESULT"
        
        # Get all values for this test
        all_values = list(performance_data[test_id].values()) if test_id in performance_data else []
        if len(all_values) < 2:
            threshold_check = config.get('threshold', 0)
            if config['higher_better']:
                rating = "EXCELLENT" if value >= threshold_check else "GOOD"
            else:
                rating = "EXCELLENT" if value <= threshold_check else "GOOD"
            precision = config.get('precision', 3)
            return f"{value:.{precision}f}{config['unit']}", rating
        
        # Calculate rank and percentage
        sorted_values = sorted(all_values, reverse=config['higher_better'])
        rank = sorted_values.index(value) + 1
        best_value = sorted_values[0]
        
        if config['higher_better']:
            pct = (value / best_value * 100) if best_value > 0 else 0
            if rank == 1:
                rating = "EXCELLENT (BEST)"
            elif pct >= 90:
                rating = "EXCELLENT (90%+)"
            elif pct >= 75:
                rating = "GOOD (75-89%)"
            elif pct >= 50:
                rating = "AVERAGE (50-74%)"
            else:
                rating = "POOR (<50%)"
        else:
            pct = (best_value / value * 100) if value > 0 else 0
            if rank == 1:
                rating = "EXCELLENT (BEST)"
            elif pct >= 90:
                rating = "EXCELLENT (90%+)"
            elif pct >= 75:
                rating = "GOOD (75-89%)"
            elif pct >= 50:
                rating = "AVERAGE (50-74%)"
            else:
                rating = "POOR (<50%)"
        
        precision = config.get('precision', 3)
        return f"{value:.{precision}f}{config['unit']}", rating
    
    def export_enhanced_csv(self, filename: str = 'comprehensive_results.csv'):
        """Export enhanced CSV with performance analysis"""
        performance_data = self.collect_performance_data()
        
        with open(filename, 'w', newline='', encoding='utf-8') as csvfile:
            writer = csv.writer(csvfile)
            
            # Header section
            writer.writerow(['Anti-Abuse System Database Performance Analysis'])
            writer.writerow(['Generated:', datetime.now().strftime('%Y-%m-%d %H:%M:%S')])
            writer.writerow([''])
            writer.writerow(['Performance Rating Guide:'])
            writer.writerow(['EXCELLENT (BEST) = Top performer among tested databases'])
            writer.writerow(['EXCELLENT (90%+) = Within 90% performance of the best'])
            writer.writerow(['GOOD (75-89%) = Within 75-89% performance of the best'])
            writer.writerow(['AVERAGE (50-74%) = Within 50-74% performance of the best'])
            writer.writerow(['POOR (<50%) = Below 50% performance of the best'])
            writer.writerow(['FAILED = Test failed to complete'])
            writer.writerow(['STRATEGY_RESULT = Shows architectural choice winner'])
            writer.writerow([''])
            
            # Main performance comparison table
            writer.writerow(['DATABASE PERFORMANCE COMPARISON'])
            
            # Create headers
            headers = ['Database']
            for test_id in sorted(self.test_configs.keys()):
                test_name = self.test_configs[test_id]['name']
                headers.extend([f'Test {test_id}: {test_name} (Value)', f'Test {test_id}: Rating'])
            writer.writerow(headers)
            
            # Data rows
            for db_name, db_results in self.results.items():
                if 'error' in db_results:
                    continue
                
                row = [db_name]
                for test_id in sorted(self.test_configs.keys()):
                    # Find corresponding test result
                    found_result = None
                    for test_name, result in db_results.items():
                        if isinstance(result, dict) and result.get('test_id') == test_id:
                            found_result = result
                            break
                    
                    if found_result and found_result.get('success', False):
                        metric = self.test_configs[test_id]['metric']
                        if metric in found_result:
                            value = found_result[metric]
                            if self.test_configs[test_id].get('multiply'):
                                value *= self.test_configs[test_id]['multiply']
                            value_str, rating = self.get_performance_rating(test_id, db_name, value, performance_data)
                            row.extend([value_str, rating])
                        else:
                            row.extend(['N/A', 'NO_DATA'])
                    else:
                        row.extend(['FAILED', 'FAILED'])
                
                writer.writerow(row)
            
            # Summary section
            writer.writerow([''])
            writer.writerow(['PERFORMANCE SUMMARY BY TEST'])
            writer.writerow(['Test', 'Best Database', 'Best Performance', 'Worst Database', 'Worst Performance', 'Performance Gap'])
            
            for test_id, config in self.test_configs.items():
                if test_id in performance_data and len(performance_data[test_id]) >= 2:
                    values = performance_data[test_id]
                    if config['metric'] == 'winner':
                        continue  # Skip strategy comparison in numeric summary
                    
                    sorted_items = sorted(values.items(), key=lambda x: x[1], reverse=config['higher_better'])
                    best_db, best_val = sorted_items[0]
                    worst_db, worst_val = sorted_items[-1]
                    
                    if config['higher_better']:
                        gap = f'{(best_val/worst_val):.1f}x faster' if worst_val > 0 else 'N/A'
                    else:
                        gap = f'{(worst_val/best_val):.1f}x slower' if best_val > 0 else 'N/A'
                    
                    precision = config.get('precision', 3)
                    writer.writerow([
                        f'Test {test_id}: {config["name"]}',
                        best_db,
                        f'{best_val:.{precision}f}{config["unit"]}',
                        worst_db,
                        f'{worst_val:.{precision}f}{config["unit"]}',
                        gap
                    ])
            
            # Anti-abuse system relevance - dynamically generated from test metadata
            writer.writerow([''])
            writer.writerow(['ANTI-ABUSE SYSTEM RELEVANCE'])
            writer.writerow(['Test ID', 'Test Name', 'Anti-Abuse System Usage', 'Business Impact', 'Category'])
            
            # Generate relevance descriptions from test metadata
            for test_id in sorted(self.test_configs.keys()):
                config = self.test_configs[test_id]
                
                # Create usage description from test info
                usage_description = config.get('description', f'Tests {config["name"].lower()}')
                
                # Map importance to business impact
                importance_mapping = {
                    'critical': 'CRITICAL - Essential for system operation',
                    'high': 'HIGH - Important for performance',
                    'medium': 'MEDIUM - Useful for optimization',
                    'low': 'LOW - Optional enhancement'
                }
                
                business_impact = importance_mapping.get(config['importance'], 'MEDIUM - Standard operation')
                
                writer.writerow([
                    f'Test {test_id}', 
                    config['name'], 
                    usage_description,
                    business_impact,
                    config['category'].title().replace('_', ' ')
                ])
        
        return {
            'filename': filename,
            'summary': 'Enhanced performance analysis exported successfully',
            'contents': [
                'Performance ratings (EXCELLENT/GOOD/AVERAGE/POOR)',
                'Percentage comparisons relative to best performer', 
                'Business relevance for anti-abuse system',
                'Performance gaps between databases'
            ]
        }