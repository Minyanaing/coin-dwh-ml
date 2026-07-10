{% macro reset_snapshots() %}
  {#-
    Drop the dbt snapshot table(s) so a subsequent `dbt build --full-refresh`
    rebuilds SCD Type-2 history from scratch.

    `--full-refresh` rebuilds incremental models but deliberately leaves
    snapshots alone (their history is meant to persist), so this is the only way
    to reset them. USE WITH CARE — it discards all accumulated SCD2 history in
    snap_coin. Snapshots live in <database>.silver_crypto (see dbt_project.yml).

    Usage:
      dbt run-operation reset_snapshots --target dev --profiles-dir .
  -#}
  {% set fqn = target.database ~ '.silver_crypto.snap_coin' %}
  {% if execute %}
    {{ log("reset_snapshots: dropping snapshot table " ~ fqn, info=true) }}
    {% do run_query('drop table if exists ' ~ fqn) %}
    {{ log("reset_snapshots: done — next `dbt build --full-refresh` re-initializes it", info=true) }}
  {% endif %}
{% endmacro %}
