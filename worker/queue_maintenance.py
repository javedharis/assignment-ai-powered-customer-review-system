import os
import time
import logging
from typing import Dict, Any
from dotenv import load_dotenv
from logic.reliable_redis_queue import ReliableRedisQueue

load_dotenv()

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class QueueMaintenanceWorker:
    """
    Standalone worker for queue maintenance tasks.
    Handles retry processing, cleanup of expired messages, and monitoring.
    """
    
    def __init__(self, maintenance_interval: int = 30):
        self.redis_queue = ReliableRedisQueue()
        self.maintenance_interval = maintenance_interval
        self.is_running = False
        
        logger.info(f"Queue maintenance worker initialized with {maintenance_interval}s interval")
    
    def start(self):
        """Start the maintenance worker"""
        logger.info("Starting queue maintenance worker")
        self.is_running = True
        
        try:
            while self.is_running:
                self.run_maintenance_cycle()
                time.sleep(self.maintenance_interval)
        except KeyboardInterrupt:
            logger.info("Received keyboard interrupt, stopping maintenance worker")
        finally:
            self.stop()
    
    def stop(self):
        """Stop the maintenance worker"""
        logger.info("Stopping queue maintenance worker")
        self.is_running = False
    
    def run_maintenance_cycle(self):
        """Run a single maintenance cycle"""
        try:
            start_time = time.time()
            
            # Check Redis connection
            if not self.redis_queue.is_connected():
                logger.error("Redis connection lost, skipping maintenance cycle")
                return
            
            # Process retry queue
            retry_count = self.redis_queue.process_retry_queue()
            
            # Cleanup expired messages
            cleanup_count = self.redis_queue.cleanup_expired_messages()
            
            # Get queue statistics
            stats = self.redis_queue.get_queue_stats()
            
            duration = time.time() - start_time
            
            # Log maintenance results
            if retry_count > 0 or cleanup_count > 0:
                logger.info(f"Maintenance cycle completed in {duration:.2f}s: "
                          f"processed {retry_count} retries, "
                          f"cleaned up {cleanup_count} expired messages")
            
            # Log queue stats periodically
            if int(time.time()) % 300 == 0:  # Every 5 minutes
                self.log_queue_stats(stats)
            
            # Alert on high queue sizes
            self.check_queue_health(stats)
            
        except Exception as e:
            logger.error(f"Error in maintenance cycle: {e}")
    
    def log_queue_stats(self, stats: Dict[str, int]):
        """Log detailed queue statistics"""
        logger.info("Queue Statistics:")
        logger.info(f"  Main queue: {stats.get('main_queue', 0)} messages")
        logger.info(f"  Processing queue: {stats.get('processing_queue', 0)} messages")
        logger.info(f"  Failed queue: {stats.get('failed_queue', 0)} messages")
        logger.info(f"  Retry queue: {stats.get('retry_queue', 0)} messages")
        logger.info(f"  Processing keys: {stats.get('processing_keys', 0)} active")
        
        total_messages = sum(stats.values())
        logger.info(f"  Total messages in system: {total_messages}")
    
    def check_queue_health(self, stats: Dict[str, int]):
        """Check queue health and alert on issues"""
        issues = []
        
        # Check for high queue sizes
        if stats.get('main_queue', 0) > 1000:
            issues.append(f"Main queue has {stats['main_queue']} messages (high backlog)")
        
        if stats.get('processing_keys', 0) > 100:
            issues.append(f"Too many processing messages ({stats['processing_keys']})")
        
        if stats.get('failed_queue', 0) > 50:
            issues.append(f"High number of failed messages ({stats['failed_queue']})")
        
        if stats.get('retry_queue', 0) > 100:
            issues.append(f"High number of retry messages ({stats['retry_queue']})")
        
        # Alert on issues
        if issues:
            logger.warning("Queue health issues detected:")
            for issue in issues:
                logger.warning(f"  - {issue}")
    
    def get_health_status(self) -> Dict[str, Any]:
        """Get health status of queue maintenance system"""
        try:
            redis_connected = self.redis_queue.is_connected()
            stats = self.redis_queue.get_queue_stats() if redis_connected else {}
            
            is_healthy = (
                self.is_running and
                redis_connected and
                stats.get('processing_keys', 0) < 200  # Alert threshold
            )
            
            return {
                'healthy': is_healthy,
                'running': self.is_running,
                'redis_connected': redis_connected,
                'queue_stats': stats,
                'timestamp': time.time()
            }
            
        except Exception as e:
            logger.error(f"Error getting health status: {e}")
            return {
                'healthy': False,
                'running': self.is_running,
                'redis_connected': False,
                'error': str(e),
                'timestamp': time.time()
            }
    
    def force_cleanup_all(self) -> Dict[str, int]:
        """Force cleanup of all queues (emergency use only)"""
        logger.warning("FORCE CLEANUP: Clearing all queues and processing state")
        
        try:
            stats_before = self.redis_queue.get_queue_stats()
            success = self.redis_queue.clear_all_queues()
            stats_after = self.redis_queue.get_queue_stats()
            
            if success:
                logger.info("Force cleanup completed successfully")
                return {
                    'success': True,
                    'cleared_main_queue': stats_before.get('main_queue', 0),
                    'cleared_processing_queue': stats_before.get('processing_queue', 0),
                    'cleared_failed_queue': stats_before.get('failed_queue', 0),
                    'cleared_retry_queue': stats_before.get('retry_queue', 0),
                    'cleared_processing_keys': stats_before.get('processing_keys', 0)
                }
            else:
                return {'success': False, 'error': 'Failed to clear queues'}
                
        except Exception as e:
            logger.error(f"Error in force cleanup: {e}")
            return {'success': False, 'error': str(e)}


if __name__ == '__main__':
    """
    Entry point for running queue maintenance worker
    """
    import sys
    
    maintenance_interval = 30  # Default 30 seconds
    if len(sys.argv) > 1:
        try:
            maintenance_interval = int(sys.argv[1])
        except ValueError:
            logger.error("Invalid maintenance interval specified")
            sys.exit(1)
    
    maintenance_worker = QueueMaintenanceWorker(maintenance_interval=maintenance_interval)
    maintenance_worker.start()