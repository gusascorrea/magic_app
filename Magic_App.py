import fundamentus as fd
import pandas as pd
import numpy as np
#import warnings
#from io import StringIO
import streamlit as st

st.title('Lista de 20 Ações Mais Baratas')
st.markdown('---')

st.subheader('Investimento')

valor_total = st.number_input('Adicione aqui o valor que deseja investir na estratégia:', value = 0)

st.markdown('---')

st.subheader('Filtro de Volume Financeiro')

vol_min = st.number_input('Digite o volume financeiro mínimo dos útimos 2 meses:', value = 700000)


gerar = st.button('Gerar Lista de Ações')

if gerar:
    # Removendo financeiras menos WIZC3
    df= fd.get_resultado_raw()

    for i in range(1, 51):
        fin = fd.list_papel_setor(i)  # finance
        if 'BBAS3' in fin:
            print(i)
            fin = fin
            break

    for i in range(1, 51):
        seg = fd.list_papel_setor(i)  # finance
        if 'WIZC3' in seg:
            print(i)
            financeiras = fin + seg
            break

    financeiras.remove('WIZC3')

    df = df[~df.index.isin(financeiras)]

    # Mantendo Margem Ebit Apenas Maior que Zero

    df = df.loc[df['Mrg Ebit'] > 0]

    # Mantendo Volume Financeiro Apenas Maior que 500k a cada 2 meses

    df = df.loc[df['Liq.2meses'] >= vol_min]
    #st.write(df['Liq.2meses'].unique())

    # Step 1: Extract the first 4 characters from the index
    df['First4Chars'] = df.index.str[:4]

    # Step 2: Determine duplicates in the first 4 characters of the index
    duplicates = df.duplicated(subset='First4Chars', keep=False)

    # Step 3: For each group of duplicates, identify the row with the maximum 'Liq.2meses' value
    max_values = df.groupby('First4Chars')['Liq.2meses'].transform('max')

    # Step 4: Filter the DataFrame to keep only the rows with the maximum 'Liq.2meses' value for each group of duplicates
    filtered_df = df[(~duplicates) | (df['Liq.2meses'] == max_values)]

    # Drop the temporary column used for grouping
    filtered_df = filtered_df.drop(columns='First4Chars')

    sorted_df = filtered_df.sort_values(by=['EV/EBIT', 'Liq.2meses'], ascending=[True, False])

    sorted_df['Quantidade'] = round((valor_total/20)/sorted_df['Cotação'],0)

    sorted_df['Valor'] = sorted_df['Quantidade'] * sorted_df['Cotação']

    st.write(sorted_df[['Cotação','EV/EBIT', 'Liq.2meses', 'Quantidade', 'Valor']].head(20))

