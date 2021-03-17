# {{ dataset.name }}
<a class="edit-link" href="/admin/#/collections/datasets/entries/{{dataset.connection}}/{{dataset.database}}/{{dataset.name}}">Edit</a>
<div class="attribute">
    <h4>Type:</h4> <code>{{ dataset.table_type }}</code>
</div>
<div class="attribute">
    <h4>Database:</h4> <code>{{ dataset.database }}</code>
</div>
<div class="attribute">
    <h4>Location:</h4> <code>{{ dataset.location }}</code>
</div>
<div class="table-content">
    {{ dataset.description.strip() }}
</div>

{% for column in dataset.columns %}
<div class="column">
    <h5 class="column-name">{{ column.name }}</h5>
    <pre class="column-type">{{ column.column_type }}</pre>
    {% if column.description != none %}
    <div class="column-description">
    {{- column.description -}}
    </div>
    {% else %}
    <div class="column-description no-description">No description</div>
    {% endif %}
    {% if column.values is not none %}
    <div class="column-values">
        {% for column_value in column.values %}
        <span class="column-value">{{ column_value }}</span>
        {% endfor %}
    </div>
    {% endif %}
</div>
{% endfor %}
