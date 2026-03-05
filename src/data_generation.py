import numpy as np
import pandas as pd

np.random.seed(42)

ids = list(range(1, 3001))
perfis = ["heavy", "weekend", "gradual", "chronic"]
prob = [0.15, 0.20, 0.25, 0.40]


sortearPerfil = np.random.choice(perfis, size = 3000, p = prob)

df_clientes = pd.DataFrame({
    "cliente_id" : ids,
    "perfil" : sortearPerfil,
})

df_clientes["perfil"].value_counts(normalize = True)

varDias = pd.date_range(start = '2025-07-01', end = '2026-01-31', freq = 'D')

df_calendario = pd.DataFrame({
    "data" : varDias,
    "days" : varDias,
    "month" : varDias.month,
    "weekend" : varDias.weekday >= 5
})

df_final = df_clientes.merge(df_calendario, how= "cross")
print(df_final.shape)

df_final["login_flag"] = False

#para cada cliente
for cliente in df_final["cliente_id"].unique():
    perfil = df_final.loc[df_final["cliente_id"] == cliente, "perfil"].iloc[0]

    if perfil == "heavy":
        media, desvio = 17.5, 1.5
    elif perfil == "weekend":
        media, desvio = 5, 1
    elif perfil == "gradual":
        media, desvio = 15, 2
    elif perfil == "chronic":
        media, desvio = 2, 0.5
    else:
        continue

#para cada mes que cada cliente possui
    for mes in df_final.loc[df_final["cliente_id"] == cliente, "month"].unique():
        df_mes = df_final[(df_final["month"] == mes) & (df_final["cliente_id"] == cliente)]
        
        if len(df_mes) == 0:
            continue
#sorteia o numero de dias ativos baseado na media e desvio do perfil
        N = int(round(np.random.normal(media, desvio)))
        N = max(1, min(N, len(df_mes)))
#marca login_flag como True
        dias_ativos = df_mes.sample(n=N).index
        df_final.loc[dias_ativos, "login_flag"] = True


print(df_final["login_flag"].sum())

df_final.to_csv("dados_clientes.csv", index=False)
df_final.to_parquet("dados_clientes.parquet", index=False)
print("Dados salvos com sucesso!")