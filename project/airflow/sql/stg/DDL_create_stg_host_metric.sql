CREATE TABLE IF NOT EXISTS stg.host_metric (
	host_id UInt32,
	host_name String,
	host_city String,
	item_id UInt32,
	item_name String,
	value Float32,
	unix_clock UInt32
)
ENGINE = MergeTree()
ORDER BY (item_id, host_id, unix_clock)
PARTITION BY toDate(unix_clock, 'UTC');