ALTER TABLE feature_stream
    ADD COLUMN previous_version UUID;

UPDATE feature_stream as fs SET previous_version =
(SELECT prev_version FROM
  (SELECT id, lag(version) OVER (PARTITION BY collection, feature_id ORDER BY id ASC) prev_version
   FROM feature_stream as src where src.collection = fs.collection and src.feature_id = fs.feature_id) AS prev where prev.id = fs.id);