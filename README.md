# Bream

**Fault-tolerant batch processing from live data sources**

Bream enables you to process data from arbitrary sources in batches with built-in checkpoint recovery. When your process is interrupted, bream automatically resumes from where it left off - no data loss, no duplicate processing.

## Quick Start

```python
from bream.core import Source, BatchRequest, Batch, Stream

# Implement your data source
class MySource(Source):
    def __init__(self, name: str):
        self.name = name  # Required for bream tracking
    
    def read(self, batch_request: BatchRequest) -> Batch | None:
        # Read data based on offset range
        # Return None when no new data is available
        return Batch(data=my_data, read_to=current_offset)

# Set up streaming
source = MySource("my_data_source")
stream = Stream(source, stream_path="/path/to/tracking")

def process_batch(batch: Batch):
    # Your processing logic here
    print(f"Processing: {batch.data}")

stream.start(process_batch, min_batch_seconds=30)
stream.wait()  # Block until completion
```

## Key Features

- **Fault Tolerance**: Automatic recovery from interruptions
- **Exactly-Once Processing**: No data loss or duplication when implemented correctly  
- **Multi-Source Support**: Process from multiple sources as a single logical stream
- **Cloud Storage**: Works with local files, S3, GCS, and other cloud storage via `cloudpathlib`
- **Lightweight**: No heavy infrastructure required - just Python

## Use Cases

Perfect for scenarios where you need to:

- Build ETL pipelines that can recover from failures
- Process files as they arrive in a directory
- Stream data from databases with offset-based pagination

## Installation

```bash
pip install bream
```

Requires Python 3.10+

## Example

For a complete working example, see: https://github.com/adamcallison/breamtest/blob/main/filesbytimestamp.py

## Documentation

Full documentation: https://adamcallison.github.io/bream/index.html
