{% macro ids_excluded_series() -%}
    {#- IDS series out of scope for the import, as a list for `not in`. -#}
    (
        -- Population
        'SP.POP.TOTL',
        -- Debt forgiveness grants and the stock-flow reconciliation residual
        'DT.DOD.MDRI.CD',
        'DT.DOD.RSDL.CD',
        -- Annual changes in debt stocks and in interest arrears
        'DT.DOD.DECT.CD.CG',
        'DT.IXA.DPPG.CD.CG',
        -- Present value of debt
        'DT.DOD.PVLX.CD',
        'DT.DOD.PVLX.GN.ZS',
        'DT.DOD.PVLX.EX.ZS',
        -- Balance of payments, investment, remittances and reserves
        'BM.GSR.TOTL.CD',
        'BN.CAB.XOKA.CD',
        'BX.KLT.DINV.CD.DT',
        'BX.KLT.DREM.CD.DT',
        'BX.PEF.TOTL.CD.DT',
        'BX.TRF.PWKR.CD.DT',
        'FI.RES.TOTL.CD',
        'FI.RES.TOTL.DT.ZS',
        'FI.RES.TOTL.MO',
        -- Exports of goods, services and primary income, and debt ratios that are
        -- shares of it
        'BX.GSR.TOTL.CD',
        'DT.DOD.DECT.EX.ZS',
        'DT.INT.DECT.EX.ZS',
        'DT.TDS.DECT.EX.ZS'
    )
{%- endmacro %}
