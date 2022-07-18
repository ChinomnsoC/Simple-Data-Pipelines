# My-First-Data-Pipeline

My first attempt at implementing a smart data pipeline with Luigi and Python.

## NAME OF FILE

#### Type of task:

- Some text

#### Description:

- Creates a pipeline that...
- The steps...

## parallel-downloads

#### Type of task:

- Parallel task

#### Description:

- Creates a pipeline that downloads 3 csv files, and uploads them to an S3 bucket in AWS
- Task 1 and Task 2 run in parallel,then task 3 runs dependent on the completion of both task one and two.

## csv-to-sql

#### Type of task:

- Parallel and successive task

#### Description:

- Creates a pipeline that merges two csv files `france.csv` and `germany.csv` into a new SQLite Database `test.db`.
- Task 1 and Task 2 run in parallel,then task 3 runs dependent on the completion of both task one and two.
