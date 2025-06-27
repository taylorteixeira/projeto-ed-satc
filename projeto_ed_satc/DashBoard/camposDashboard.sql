WITH kpi_mrr_data AS (
  SELECT
    MES_ANO,
    SUM(VALOR_PAGO_MES) AS MRR
  FROM gold.fato_resumo_usuario_mensal
  GROUP BY MES_ANO
),

metrica_novos_usuarios_data AS (
  SELECT
    date_format(DATA_CADASTRO, 'yyyy-MM') AS MES_ANO,
    COUNT(ID_USUARIO) AS NOVOS_USUARIOS
  FROM gold.dim_usuario
  GROUP BY date_format(DATA_CADASTRO, 'yyyy-MM')
),

metrica_minutos_assistidos_data AS (
  SELECT
    MES_ANO,
    SUM(MINUTOS_ASSISTIDOS_MES) AS TOTAL_MINUTOS_ASSISTIDOS
  FROM gold.fato_resumo_usuario_mensal
  GROUP BY MES_ANO
),

atividade_usuario_mes AS (
  SELECT DISTINCT FK_USUARIO, MES_ANO, FLAG_ATIVO_MES
  FROM gold.fato_resumo_usuario_mensal
),

atividade_com_mes_anterior AS (
  SELECT
    FK_USUARIO,
    MES_ANO,
    FLAG_ATIVO_MES,
    LAG(FLAG_ATIVO_MES, 1, 0) OVER (PARTITION BY FK_USUARIO ORDER BY MES_ANO) AS ATIVO_MES_ANTERIOR
  FROM atividade_usuario_mes
),

kpi_churn_rate_data AS (
  SELECT
    MES_ANO,
    SUM(ATIVO_MES_ANTERIOR) AS TOTAL_ATIVOS_MES_ANTERIOR,
    SUM(CASE WHEN ATIVO_MES_ANTERIOR = 1 AND FLAG_ATIVO_MES = 0 THEN 1 ELSE 0 END) AS CHURNED_USERS,
    CASE 
      WHEN SUM(ATIVO_MES_ANTERIOR) > 0 THEN 
        (SUM(CASE WHEN ATIVO_MES_ANTERIOR = 1 AND FLAG_ATIVO_MES = 0 THEN 1 ELSE 0 END) / SUM(ATIVO_MES_ANTERIOR)) * 100
      ELSE 0 
    END AS TAXA_DE_CHURN_PERCENTUAL
  FROM atividade_com_mes_anterior
  GROUP BY MES_ANO
),

arpu_data AS (
  SELECT
    k.MES_ANO,
    k.MRR / NULLIF(c.TOTAL_ATIVOS_MES_ANTERIOR, 0) AS ARPU_MENSAL,
    c.TAXA_DE_CHURN_PERCENTUAL
  FROM kpi_mrr_data k
  JOIN kpi_churn_rate_data c ON k.MES_ANO = c.MES_ANO
),

kpi_ltv_data AS (
  SELECT
    MES_ANO,
    CASE 
      WHEN TAXA_DE_CHURN_PERCENTUAL > 0 THEN 
        ARPU_MENSAL / (TAXA_DE_CHURN_PERCENTUAL / 100)
      ELSE NULL 
    END AS LTV_ESTIMADO
  FROM arpu_data
),

engajamento_pais_plano_data AS (
  SELECT
    f.MES_ANO,
    u.PAIS,
    p.NOME_PLANO,
    AVG(f.MINUTOS_ASSISTIDOS_MES) AS MEDIA_MINUTOS_ASSISTIDOS,
    COUNT(DISTINCT f.FK_USUARIO) AS USUARIOS_ATIVOS
  FROM gold.fato_resumo_usuario_mensal f
  JOIN gold.dim_usuario u ON f.FK_USUARIO = u.ID_USUARIO
  JOIN gold.dim_plano p ON f.FK_PLANO = p.ID_PLANO
  WHERE f.FLAG_ATIVO_MES = 1
  GROUP BY f.MES_ANO, u.PAIS, p.NOME_PLANO
)

-- Dashboard principal com todas as métricas consolidadas
SELECT 
  COALESCE(m.MES_ANO, nu.MES_ANO, ma.MES_ANO, cr.MES_ANO, ltv.MES_ANO) AS MES_ANO,
  
  -- KPIs principais
  COALESCE(m.MRR, 0) AS MRR,
  COALESCE(cr.TAXA_DE_CHURN_PERCENTUAL, 0) AS TAXA_CHURN_PERCENTUAL,
  COALESCE(ltv.LTV_ESTIMADO, 0) AS LTV_ESTIMADO,
  
  -- Métricas de crescimento
  COALESCE(nu.NOVOS_USUARIOS, 0) AS NOVOS_USUARIOS,
  COALESCE(ma.TOTAL_MINUTOS_ASSISTIDOS, 0) AS TOTAL_MINUTOS_ASSISTIDOS,
  
  -- Métricas calculadas adicionais
  COALESCE(cr.TOTAL_ATIVOS_MES_ANTERIOR, 0) AS USUARIOS_ATIVOS,
  CASE 
    WHEN cr.TOTAL_ATIVOS_MES_ANTERIOR > 0 THEN 
      m.MRR / cr.TOTAL_ATIVOS_MES_ANTERIOR 
    ELSE 0 
  END AS ARPU,
  
  CASE 
    WHEN cr.TOTAL_ATIVOS_MES_ANTERIOR > 0 THEN 
      ma.TOTAL_MINUTOS_ASSISTIDOS / cr.TOTAL_ATIVOS_MES_ANTERIOR 
    ELSE 0 
  END AS MINUTOS_POR_USUARIO

FROM kpi_mrr_data m
FULL OUTER JOIN metrica_novos_usuarios_data nu ON m.MES_ANO = nu.MES_ANO
FULL OUTER JOIN metrica_minutos_assistidos_data ma ON COALESCE(m.MES_ANO, nu.MES_ANO) = ma.MES_ANO
FULL OUTER JOIN kpi_churn_rate_data cr ON COALESCE(m.MES_ANO, nu.MES_ANO, ma.MES_ANO) = cr.MES_ANO
FULL OUTER JOIN kpi_ltv_data ltv ON COALESCE(m.MES_ANO, nu.MES_ANO, ma.MES_ANO, cr.MES_ANO) = ltv.MES_ANO

ORDER BY MES_ANO;