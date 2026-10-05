{% macro ids_code_lookup(alias, lists, property, code) -%}
    {#-
        A left join resolving one World Bank series-code segment to the DCID of the
        ontology node it names.

        The IDS mapping needs more than twenty of these, differing only in which classes
        they read, which property publishes the code, and which segment they match. Written
        out they would be a hundred lines of near-identical join, so they are
        parameterised here.

        `lists` takes one class or several: a lookup spans more than one where the
        nodes it reaches are of different kinds, as the lender lookup does over
        organisations and the groupings that no organisation covers.

        Scoping on the property as well as the class is needed. code_mapping
        publishes some IDS codes under wbCode and some under classificationCode, and a
        join that names the wrong one does not error - it misses, resulting in a
        StatVar that is silently under-specified.
    -#}
    {%- set classes = [lists] if lists is string else lists %}
    left join {{ ref("code_mapping") }} as {{ alias }}
        on
            {{ alias }}.list in ({% for class in classes %}'{{ class }}'{% if not loop.last %}, {% endif %}{% endfor %})
            and {{ alias }}.property = '{{ property }}'
            and {{ code }} = {{ alias }}.code
{%- endmacro %}
