import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

np.random.seed(45)
random.seed(45)

n_leads = 1000

# Генерируем синтетические данные лидов CRM
leads = []
start_date = datetime(2024, 1, 1)
end_date = datetime(2024, 7, 25)

for i in range(n_leads):
    submission_date = start_date + timedelta(days=random.randint(0, (end_date - start_date).days))
    source = random.choice(["Google Ads", "Facebook Ads", "Organic Search", "Referral", "Direct"])
    status = random.choice(["New", "Contacted", "Qualified", "Unqualified"])
    
    leads.append({
        "lead_id": i + 1,
        "submission_date": submission_date,
        "email": f"user{i+1}@example.com",
        "phone": f"+79{random.randint(100000000, 999999999)}",
        "source": source,
        "status": status,
        "value": round(random.uniform(1000, 50000), 2) if status == "Qualified" else 0
    })

leads_df = pd.DataFrame(leads)
leads_df.to_csv("crm_leads_raw.csv", index=False)

print("Синтетические данные лидов CRM успешно сгенерированы!")
print(f"Всего лидов: {len(leads_df)}")


