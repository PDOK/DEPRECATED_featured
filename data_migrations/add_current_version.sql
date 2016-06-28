ALTER TABLE feature_stream
    ADD COLUMN previous_version UUID;


-- set previous version UPDATE STYLE
UPDATE feature_stream as fs SET previous_version =
(SELECT prev_version FROM
  (SELECT id, lag(version) OVER (PARTITION BY collection, feature_id ORDER BY id ASC) prev_version
   FROM feature_stream as src where src.collection = fs.collection and src.feature_id = fs.feature_id) AS prev where prev.id = fs.id);

-- set previous version LOOP STYLE
DO
$$
DECLARE
  ref REFCURSOR;
  row RECORD;
  prev_version UUID;
BEGIN
  OPEN ref FOR EXECUTE 'SELECT * FROM feature_stream';
  LOOP
    FETCH NEXT FROM ref INTO row;
    EXIT WHEN row is null;

    SELECT version INTO prev_version FROM (
                                            SELECT src.id, lag(version) OVER (PARTITION BY collection, feature_id ORDER BY id ASC) AS version
                                            FROM feature_stream as src
                                            WHERE src.collection = row.collection
                                                  AND src.feature_id = row.feature_id
                                            ORDER BY src.id) AS r
    WHERE r.id = row.id;

    UPDATE feature_stream SET previous_version = prev_version WHERE CURRENT OF ref;
  END LOOP;
END;
$$;