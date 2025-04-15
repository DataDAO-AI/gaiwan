import logging
import time
from pathlib import Path
from gaiwan.automation.config import Config
from gaiwan.automation.process_manager import ProcessManager

def setup_logging(log_level: str = "INFO") -> None:
    """Set up logging configuration."""
    logging.basicConfig(
        level=getattr(logging, log_level),
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('automation.log'),
            logging.StreamHandler()
        ]
    )

def main():
    # Load configuration
    config = Config.from_file('automation_config.json')
    
    # Set up logging
    setup_logging(config.log_level)
    logger = logging.getLogger(__name__)
    
    # Initialize ProcessManager
    process_manager = ProcessManager(config)
    
    try:
        # Get number of partitions based on available resources
        num_partitions = config.max_processes or max(1, len(list(config.archives_dir.glob('*_archive.json'))) // 10)
        logger.info(f"Will partition files into {num_partitions} folders")
        
        # Start analysis for all partitions
        if process_manager.start_analysis(num_partitions):
            logger.info("Successfully started analysis for all partitions")
        else:
            logger.error("Failed to start analysis")
            return
            
        # Monitor processes and handle failures
        while process_manager.active_processes:
            process_manager.monitor_processes()
            process_manager.handle_failures()
            
            # Get and log status
            status = process_manager.get_status()
            logger.info(f"Current status: {status}")
            
            # Wait before next check
            time.sleep(5)
            
        logger.info("All partitions processed successfully")
        
    except KeyboardInterrupt:
        logger.info("Received interrupt signal, cleaning up...")
    except Exception as e:
        logger.error(f"Error during processing: {e}")
    finally:
        # Clean up any remaining processes
        process_manager.cleanup()
        logger.info("Cleanup complete")

if __name__ == "__main__":
    main() 