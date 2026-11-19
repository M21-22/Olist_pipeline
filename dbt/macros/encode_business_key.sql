{#
  Match airflow/include/reconcile.py::encode_key for the non-null Olist key
  types: one column -> plain text; composite -> JSON array with jsonb
  whitespace, e.g. ["order-1", 2]. Do not normalize key text.
  The audit.deleted_keys ledger is not currently populated by reconciliation;
  this macro only standardizes its existing read-side contract.
#}
{% macro encode_business_key(columns) -%}
    {%- if columns | length == 1 -%}
        ({{ columns[0] }})::text
    {%- elif columns | length > 1 -%}
        jsonb_build_array({{ columns | join(', ') }})::text
    {%- else -%}
        {{ exceptions.raise_compiler_error('encode_business_key requires at least one column') }}
    {%- endif -%}
{%- endmacro %}
