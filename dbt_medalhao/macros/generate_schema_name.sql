{#
    =========================================================================
     Sobrescreve como o dbt monta o nome do schema de destino.

     ── O PROBLEMA ──────────────────────────────────────────────────────────
     O comportamento padrão do dbt CONCATENA:

         schema final = <schema do profile> _ <schema do modelo>

     Com o nosso profile (schema: silver) e um modelo marcado `+schema: gold`,
     o dbt criaria a tabela em:

         silver_gold.minha_tabela        ← errado

     Esse padrão existe para times que compartilham um banco: cada dev roda
     com o próprio prefixo e ninguém sobrescreve o modelo do outro. Faz todo
     sentido lá — e destrói a arquitetura medalhão aqui, onde os nomes dos
     schemas SÃO a arquitetura.

     ── A CORREÇÃO ──────────────────────────────────────────────────────────
     Usar o schema declarado no modelo exatamente como está:

         +schema: gold   →   gold.minha_tabela        ✅
         +schema: silver →   silver.minha_tabela      ✅
         (sem +schema)   →   o schema do profile

     Esta é uma macro "built-in" do dbt: criar um arquivo com este nome
     substitui a implementação padrão. Não precisa registrar em lugar nenhum.
    =========================================================================
#}

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}

{%- endmacro %}
