{% macro mint_statvar_dcid(
    stat_type_dcid_segment, measured_property, population_type, constraints,
    measurement_qualifier, measurement_denominator=none
) %}
    {#-
        StatVar DCID following the DFO convention:
        statType_measuredProperty_populationType_[constraints]_measurementQualifier
        [_AsAFractionOf_measurementDenominator]

        array_to_string omits a null segment along with its separator, so a stat type
        contributing no segment, or a constraint that does not apply to a measure, leaves
        no empty segment behind.

        Constraints are an ordered mapping of constraint property DCID to the column
        holding its value DCID.

        A ratio variable is otherwise indistinguishable from the level variable it
        divides - interest payments and interest payments as a share of exports carry
        the same population, measure and constraints - so a denominator contributes a
        trailing `AsAFractionOf` segment, as Data Commons names such variables. A
        pipeline measuring no ratio passes no denominator and mints the same DCID it
        did before.

        The marker and the denominator are one element rather than two, carrying their
        own separator: array_to_string drops a null but not a literal, so a marker of
        its own would trail every variable that divides by nothing.
    -#}
    'dcid:' || array_to_string(
        [
            {{ stat_type_dcid_segment }},
            upper(left({{ strip_dcid_namespace(measured_property) }}, 1)) || substr({{ strip_dcid_namespace(measured_property) }}, 2),
            {{ strip_dcid_namespace(population_type) }}
            {%- for constraint in constraints.values() %},
            {{ strip_dcid_namespace(constraint) }}
            {%- endfor %},
            {{ strip_dcid_namespace(measurement_qualifier) }}
            {%- if measurement_denominator is not none %},
            case
                when {{ measurement_denominator }} is not null
                    then 'AsAFractionOf_' || {{ strip_dcid_namespace(measurement_denominator) }}
            end
            {%- endif %}
        ],
        '_'
    )
{%- endmacro %}
