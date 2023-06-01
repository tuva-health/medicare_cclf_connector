select 
    p.state, 
    AVG(rs.raw_risk_score) as avg_score 
from {{ref('core__patient')}} p
inner join {{ref('cms_hcc__patient_risk_scores')}} rs USING (patient_id)
group by 1
