


# Architecture Decision Record

## 1. Context
Need to process S3 objects in chunks with state tracking between Lambda invocations.

## 2. Decision
Use DynamoDB for state management with the following structure:
- PartitionKey: Composite key (OWNER#<owner>#EXECUTION_ID#<id>)
- EvenType: Event category (Count/Report)
- DateTime: Processing timestamp
- Message: Event payload

## 3. Consequences
- **Positive**:
  - Resilient to Lambda timeouts
  - Track processing progress
  - Avoid reprocessing
- **Negative**:
  - Additional DynamoDB costs
  - Increased complexity

## 4. Alternatives Considered
1. **S3 Inventory**: Too slow for real-time tracking
2. **Step Functions Map State**: Cost-prohibitive for large datasets
3. **Redis**: Requires managing additional infrastructure

## 5. Implementation Notes
- AWS SDK v2 for better pagination handling
- Rome timezone for consistent timestamps
- Chunk size configuration via environment variable
- Custom exception handling for input validation