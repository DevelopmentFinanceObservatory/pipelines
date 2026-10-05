{% macro ids_excluded_series() -%}
    {#-
        IDS series out of scope for the import, as a list for `not in`.

        - Population (SP.POP.TOTL): Data Commons already has it.
        - Debt forgiveness grants (DT.DOD.MDRI.CD) and the stock-flow reconciliation
          residual (DT.DOD.RSDL.CD): removed at the ontology author's request.
          Without their flow types they name no dimension, so they are not Data
          Commons variables.
        - The annual changes in debt stocks (DT.DOD.DECT.CD.CG) and in interest
          arrears (DT.IXA.DPPG.CD.CG): the ontology has no term for a change in an
          amount, and without one each would be published as the level it changes.
        - The present value of debt (DT.DOD.PVLX.CD, .GN.ZS and .EX.ZS): the
          ontology has no term for it yet. It covers public and publicly guaranteed
          debt only, so it needs that guarantee status when it is added.
        - Nine context series (balance of payments, direct investment, portfolio
          flows, reserves and their ratios): not debt statistics. GNI and exports
          are kept, because the debt ratios are denominated by them.
    -#}
    (
        'SP.POP.TOTL',
        'DT.DOD.MDRI.CD',
        'DT.DOD.RSDL.CD',
        'DT.DOD.DECT.CD.CG',
        'DT.IXA.DPPG.CD.CG',
        'DT.DOD.PVLX.CD',
        'DT.DOD.PVLX.GN.ZS',
        'DT.DOD.PVLX.EX.ZS',
        'BM.GSR.TOTL.CD',
        'BN.CAB.XOKA.CD',
        'BX.KLT.DINV.CD.DT',
        'BX.KLT.DREM.CD.DT',
        'BX.PEF.TOTL.CD.DT',
        'BX.TRF.PWKR.CD.DT',
        'FI.RES.TOTL.CD',
        'FI.RES.TOTL.DT.ZS',
        'FI.RES.TOTL.MO'
    )
{%- endmacro %}
