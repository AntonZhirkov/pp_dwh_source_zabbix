-- VIEW хранит данные за предыдущий час
DROP VIEW IF EXISTS public.outbox;

CREATE VIEW public.outbox AS
WITH get_hosts AS (
	SELECT
		hostid,
		host,
		description
	FROM hosts
	WHERE host LIKE 'server%'
	ORDER BY hostid
),
get_items AS (
	SELECT
		hostid,
		itemid,
		name
	FROM items
	WHERE name IN (
		'ICMP ping',
		'CPU utilization',
		'Interface {#IFNAME}({#IFALIAS}): Bits received',
		'Interface {#IFNAME}({#IFALIAS}): Bits sent'
	) AND templateid IS NOT NULL
),
get_metrics_data AS (
	SELECT
		*, TO_TIMESTAMP(clock)
	FROM history
	WHERE CAST(TO_TIMESTAMP(clock) AS DATE) = CURRENT_DATE 
		AND EXTRACT(HOUR FROM TO_TIMESTAMP(clock)) = EXTRACT(HOUR FROM NOW()) - 1
	UNION 
	SELECT
		*, TO_TIMESTAMP(clock)
	FROM history_uint
	WHERE CAST(TO_TIMESTAMP(clock) AS DATE) = CURRENT_DATE 
		AND EXTRACT(HOUR FROM TO_TIMESTAMP(clock)) = EXTRACT(HOUR FROM NOW()) - 1
)
SELECT
	*
FROM get_hosts gh
INNER JOIN get_items gi USING(hostid)
INNER JOIN get_metrics_data gmd USING(itemid)
ORDER BY gh.hostid, gi.name, gmd.clock;