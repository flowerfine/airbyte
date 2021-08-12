
  create or replace  view "AIRBYTE_DATABASE"._AIRBYTE_TEST_NORMALIZATION."CONFLICT_STREAM_ARRAY_CONFLICT_STREAM_ARRAY_AB2"  as (
    
-- SQL model to cast each column to its adequate SQL type converted from the JSON schema type
select
    _AIRBYTE_CONFLICT_STREAM_ARRAY_HASHID,
    CONFLICT_STREAM_NAME,
    _airbyte_emitted_at
from "AIRBYTE_DATABASE"._AIRBYTE_TEST_NORMALIZATION."CONFLICT_STREAM_ARRAY_CONFLICT_STREAM_ARRAY_AB1"
-- CONFLICT_STREAM_ARRAY at conflict_stream_array/conflict_stream_array
  );