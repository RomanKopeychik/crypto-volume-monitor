-- latest loaded 5-minute candle
SELECT
  symbol AS symbol,
  SUM(volume_usdt) AS "SUM(volume_usdt)"
FROM (
  SELECT
    *
  FROM public.crypto_volume
  WHERE
    ts = (
      SELECT
        MAX(ts)
      FROM public.crypto_volume
    )
) AS virtual_table
GROUP BY
  symbol
ORDER BY
  "SUM(volume_usdt)" DESC
LIMIT 100;

-- last 24 hours
SELECT
  symbol AS symbol,
  SUM(volume_usdt) AS "sum(volume_usdt)"
FROM public.crypto_volume
WHERE
  (
    ts > CURRENT_TIMESTAMP - INTERVAL '1 DAY'
  )
GROUP BY
  symbol
ORDER BY
  "sum(volume_usdt)" DESC
LIMIT 100;
