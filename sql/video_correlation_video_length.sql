SELECT
    corr(EXTRACT(EPOCH FROM duration), view_count) AS duration_view_correlation
FROM videos
WHERE duration IS NOT NULL
  AND view_count IS NOT NULL;