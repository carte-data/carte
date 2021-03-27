# {{ dataset.name }} {: .page-title }

<a class="edit-link" target="_blank" href="/admin/#/collections/datasets/entries/{{dataset.connection}}/{{dataset.database}}/{{dataset.name}}"></a>

#### Type: {: .attribute }
`{{ dataset.table_type.value }}`

<br>
{: .attribute-break}

#### Database: {: .attribute }
`{{ dataset.database }}`

<br>
{: .attribute-break}

#### Location: {: .attribute }
`{{ dataset.location }}`

<br>
{: .attribute-break}

## Description

{% if dataset.description.strip() != '' %}
{{ dataset.description.strip() }}
{% else %}
No description
{: .no-description }
{% endif %}

## Columns

{% for column in dataset.columns %}
##### {{ column.name }} {: .column-name }

`{{ column.column_type }}`{: .column-type }
{% if column.description != none %}
{{ column.description.strip() }}
{: .column-description }

{% else %}
<br>
{: .column-name-break}

{% endif %}
{% if column.values != none %}
{% for column_value in column.values %}
{{ column_value }}
{: .column-value }
{% endfor %}

<br>
{: .column-value-break}

{% endif %}
{% endfor %}
