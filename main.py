from indexers.movies import MoviesIndexer
from indexers.series import SeriesIndexer
from concurrent.futures import ProcessPoolExecutor, as_completed
from typing import Callable
import logging
import time

def setup_logging() -> None:
    """Configure logging for the indexer processes."""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
        handlers=[
            logging.FileHandler('log/indexer.log'),
            logging.StreamHandler()
        ]
    )

def run_indexer(indexer_func: Callable, name: str) -> dict:
    """
    Execute an indexer function with error handling and logging.
    
    Args:
        indexer_func: The indexing function to execute
        name: Name of the indexer for logging purposes
    
    Returns:
        dict: Result status including success/failure and timing information
    """
    logger = logging.getLogger(name)
    start_time = time.time()
    
    try:
        logger.info(f"Starting {name} indexing process")
        indexer_func()
        execution_time = time.time() - start_time
        logger.info(f"Completed {name} indexing in {execution_time:.2f} seconds")
        return {
            "name": name,
            "status": "success",
            "execution_time": execution_time
        }
    except Exception as e:
        execution_time = time.time() - start_time
        logger.error(f"Error in {name} indexing: {str(e)}", exc_info=True)
        return {
            "name": name,
            "status": "failed",
            "error": str(e),
            "execution_time": execution_time
        }

def main():
    """Main function to run the indexing processes in parallel."""
    setup_logging()
    logger = logging.getLogger("main")
    
    indexers = [
        ("MovieIndexer", MoviesIndexer().create_index),
        ("SeriesIndexer", SeriesIndexer().create_index)
    ]
    
    results = []
    start_time = time.time()
    
    with ProcessPoolExecutor(max_workers=4) as executor:
        # Submit all indexing tasks
        future_to_indexer = {
            executor.submit(run_indexer, func, name): name 
            for name, func in indexers
        }
        
        # Process completed tasks as they finish
        for future in as_completed(future_to_indexer):
            result = future.result()
            results.append(result)
            
            if result["status"] == "success":
                logger.info(f"{result['name']} completed successfully in {result['execution_time']:.2f} seconds")
            else:
                logger.error(f"{result['name']} failed: {result.get('error')}")
    
    # Log summary
    total_time = time.time() - start_time
    successful = sum(1 for r in results if r["status"] == "success")
    failed = sum(1 for r in results if r["status"] == "failed")
    
    logger.info(f"""
    Indexing Summary:
    Total Time: {total_time:.2f} seconds
    Successful: {successful}
    Failed: {failed}
    """)
    
    return results

if __name__ == "__main__":
    main()