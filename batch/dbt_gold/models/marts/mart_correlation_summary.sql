{{ config(materialized='table', file_format='delta') }}

select
    classification,
    affected_banks,
    detected_at
from delta.`{{ var('project_root') }}/data/processed/gold_correlation_events`