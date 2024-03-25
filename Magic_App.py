import fundamentus as fd
import pandas as pd
import numpy as np
#import warnings
#from io import StringIO
import streamlit as st

language = st.sidebar.radio("Select Language", ['Português', 'English'])

if language == 'Português':

    st.sidebar.markdown('---')
    painel = st.sidebar.radio('Painel', ['Magic Formula e Value Investing', 'Lista de Ações'])
    if painel == 'Magic Formula e Value Investing':
        st.header('Magic Formula e Value Investing')
        st.markdown('---')
        st.write('A Magic Formula é uma estratégia de seleção de ativos desenvolvida por Joel\
        Greenblatt para selecionar ações com altos ROICs e Earnings Yield, representando\
        qualidade e valor respectivamente, medidos por indicadores contábeis das empresas.')

        st.subheader('Retornos Anuais')
        st.image('images/retornos_RomanGabriel.PNG')
        col1, col2, col3 = st.columns(3)
        with col2:
            st.write('Fonte: ROMAN, Gabriel. 2021.')

        st.subheader('Volatilidade Anual')
        st.image('images/volatilidade_RomanGabriel.PNG')
        col1, col2, col3 = st.columns(3)
        with col2:
            st.write('Fonte: ROMAN, Gabriel. 2021.')

        st.subheader('Dados de Performance')
        st.image('images/performance_RomanGabriel.PNG')
        col1, col2, col3 = st.columns(3)
        with col2:
            st.write('Fonte: ROMAN, Gabriel. 2021.')

    elif painel == 'Lista de Ações':
        st.header('Lista de Ações')
        st.markdown('---')

        st.subheader('Estratégia')

        estrategia = st.selectbox('Selecione a estratégia', ['Earnings Yield', 'Magic Formula', 'ROIC'])

        ativos_na_carteira = st.number_input('Quantidade de ativos na carteira:', value = 20)

        st.markdown('---')

        st.subheader('Investimento')

        valor_total = st.number_input('Adicione aqui o valor que deseja investir na estratégia:', value = 0)

        st.markdown('---')

        st.subheader('Filtro de Liquidez')

        vol_min = st.number_input('Digite o volume financeiro mínimo dos útimos 2 meses:', value = 400000)

        st.markdown('---')

        # Removendo financeiras menos WIZC3
        df= fd.get_resultado_raw()

        for i in range(1, 51):
            fin = fd.list_papel_setor(i)  # finance
            if 'BBAS3' in fin:
                fin = fin
                break

        for i in range(1, 51):
            seg = fd.list_papel_setor(i)  # finance
            if 'WIZC3' in seg:
                financeiras = fin + seg
                break

        financeiras.remove('WIZC3')

        df = df[~df.index.isin(financeiras)]

        # Removendo ADRs e BDRs

        df = df[~(df.index.astype(str).str.contains('33') | df.index.astype(str).str.contains('34'))]

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
        filtered_df['Earnings Yield'] = round(1/filtered_df['EV/EBIT'] * 100,1)
        filtered_df['ROIC'] = round(filtered_df['ROIC'] * 100, 1)

        if estrategia == 'Earnings Yield':
            st.subheader('Earnings Yield')
            st.write(f'Primeiros {ativos_na_carteira} ativos')
            sorted_df = filtered_df.sort_values(by=['EV/EBIT', 'Liq.2meses'], ascending=[True, False])

            sorted_df['Quantidade'] = round((valor_total/ativos_na_carteira)/sorted_df['Cotação'],0)

            sorted_df['Valor'] = sorted_df['Quantidade'] * sorted_df['Cotação']

            sorted_df = sorted_df[['Cotação','Earnings Yield', 'Liq.2meses', 'Quantidade', 'Valor']].head(ativos_na_carteira)

        elif estrategia == 'Magic Formula':
            st.subheader('Magic Formula')
            st.write(f'Primeiros {ativos_na_carteira} ativos')
            filtered_df['Ranking_Earning_Yield'] = filtered_df['Earnings Yield'].rank(ascending=False)
            filtered_df['Ranking_ROIC'] = filtered_df['ROIC'].rank(ascending=False)
            filtered_df['Magic Formula'] = filtered_df['Ranking_Earning_Yield'] + filtered_df['Ranking_ROIC']

            sorted_df = filtered_df.sort_values(by=['Magic Formula', 'Liq.2meses'], ascending=[True, False])

            sorted_df['Quantidade'] = round((valor_total/ativos_na_carteira)/sorted_df['Cotação'], 0)

            sorted_df['Valor'] = sorted_df['Quantidade'] * sorted_df['Cotação']

            sorted_df = sorted_df[['Cotação','Earnings Yield','ROIC', 'Magic Formula', 'Liq.2meses', 'Quantidade', 'Valor']].head(ativos_na_carteira)

        elif estrategia == 'ROIC':
            st.subheader('ROIC')
            st.write(f'Primeiros {ativos_na_carteira} ativos')
            sorted_df = filtered_df.sort_values(by=['ROIC', 'Liq.2meses'], ascending=[False, False])

            sorted_df['Quantidade'] = round((valor_total/ativos_na_carteira)/sorted_df['Cotação'], 0)

            sorted_df['Valor'] = sorted_df['Quantidade'] * sorted_df['Cotação']

            sorted_df = sorted_df[['Cotação','ROIC', 'Liq.2meses', 'Quantidade', 'Valor']].head(ativos_na_carteira)

        st.table(sorted_df.style.format(precision=2))

    elif painel == 'Rebalanceamento':
        st.header('Rebalanceamento')
        st.markdown('---')
        st.write('Escrever Rebalanceamento')

    # Referências
    st.sidebar.markdown('---')
    st.sidebar.subheader('Referências:')
    st.sidebar.write('ROMAN, Gabriel. Avaliação da Eficiência da Magic Formula e de Estratégias de Value Investing para o Mercado Brasileiro. Porto Alegre: UFRGS, 2021.')
    st.sidebar.download_button(label="Download PDF", data='references/TCC Magic Formula.pdf', file_name="TCC_Magic_Formula.pdf", mime="application/pdf")
    st.sidebar.write('ZEIDLER, Rodolfo Gunther Dias. Eficiência da Magic Formula de Value Investing no Mercado Brasileiro. São Paulo: FGV, 2014.')
    st.sidebar.download_button(label="Download PDF", data='references/FGV Magic Formula.pdf', file_name="FGV_Magic_Formula.pdf", mime="application/pdf")

elif language == 'English':
    st.sidebar.markdown('---')
    painel = st.sidebar.radio("Panel", ['Magic Formula & Value Investing', 'List of Stocks'])
    if painel == 'Magic Formula & Value Investing':
        st.header('Magic Formula & Value Investing')
        st.markdown('---')
        st.write('Magic Formula Strategy Selection Based on Magic Formula. The Magic Formula was developed by Joel\
        Greenblatt for selecting stocks with high ROICs and Earnings Yield, representing\
        quality and value respectively, measured by contabulary-based indicators for companies.')

        st.subheader('Annual Returns')
        st.image('images/retornos_RomanGabriel.PNG')
        col1, col2, col3 = st.columns(3)
        with col2:
            st.write('Source: ROMAN, Gabriel. 2021.')

        st.subheader('Annual Volatility')
        st.image('images/volatilidade_RomanGabriel.PNG')
        col1, col2, col3 = st.columns(3)
        with col2:
            st.write('Source: ROMAN, Gabriel. 2021.')

        st.subheader('Performance Data')
        st.image('images/performance_RomanGabriel.PNG')
        col1, col2, col3 = st.columns(3)
        with col2:
            st.write('Source: ROMAN, Gabriel. 2021.')

    elif painel == 'List of Stocks':
        st.header('List of Stocks')
        st.markdown('---')

        st.subheader('Strategy')

        estrategia = st.selectbox('Select the strategy', ['Earnings Yield', 'Magic Formula', 'ROIC'])

        ativos_na_carteira = st.number_input('Quantity of stocks in the portfolio:', value = 20)

        st.markdown('---')

        st.subheader('Investment')

        valor_total = st.number_input('Add here the value you want to invest in the strategy:', value = 0)

        st.markdown('---')

        st.subheader('Liquidity Filter')

        vol_min = st.number_input('Enter the volume financeiro minimo dos ultimos 2 meses:', value = 400000)

        st.markdown('---')

        # Removendo financeiras menos WIZC3
        df= fd.get_resultado_raw()

        for i in range(1, 51):
            fin = fd.list_papel_setor(i)  # finance
            if 'BBAS3' in fin:
                fin = fin
                break

        for i in range(1, 51):
            seg = fd.list_papel_setor(i)  # finance
            if 'WIZC3' in seg:
                financeiras = fin + seg
                break
                
        financeiras.remove('WIZC3')
        df = df[~df.index.isin(financeiras)]
        # Removendo ADRs e BDRs
        df = df[~(df.index.astype(str).str.contains('33') | df.index.astype(str).str.contains('34'))]
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
        filtered_df['Earnings Yield'] = round(1/filtered_df['EV/EBIT'] * 100, 1)
        filtered_df['ROIC'] = round(filtered_df['ROIC'] * 100, 1)
        filtered_df['Price'] = round(filtered_df['Cotação'], 2)
        if estrategia == 'Earnings Yield':
            st.subheader('Earnings Yield')
            st.write(f'First {ativos_na_carteira} stocks')
            sorted_df = filtered_df.sort_values(by=['EV/EBIT', 'Liq.2meses'], ascending=[True, False])
            sorted_df['Quantity'] = round((valor_total/ativos_na_carteira)/sorted_df['Price'], 0)
            sorted_df['Amount'] = sorted_df['Quantity'] * sorted_df['Price']
            sorted_df = sorted_df[['Price','Earnings Yield', 'Liq.2meses', 'Quantity', 'Amount']].head(ativos_na_carteira)
            st.table(sorted_df.style.format(precision=2))
        elif estrategia == 'Magic Formula':
            st.subheader('Magic Formula')
            st.write(f'First {ativos_na_carteira} stocks')
            filtered_df['Ranking_Earning_Yield'] = filtered_df['Earnings Yield'].rank(ascending=False)
            filtered_df['Ranking_ROIC'] = filtered_df['ROIC'].rank(ascending=False)
            filtered_df['Magic Formula'] = filtered_df['Ranking_Earning_Yield'] + filtered_df['Ranking_ROIC']
            sorted_df = filtered_df.sort_values(by=['Magic Formula', 'Liq.2meses'], ascending=[True, False])
            sorted_df['Quantity'] = round((valor_total/ativos_na_carteira)/sorted_df['Price'], 0)
            sorted_df['Amount'] = sorted_df['Quantity'] * sorted_df['Price']
            sorted_df = sorted_df[['Price','Earnings Yield','ROIC', 'Magic Formula', 'Liq.2meses', 'Quantity', 'Amount']].head(ativos_na_carteira)
            st.table(sorted_df.style.format(precision=2))
        elif estrategia == 'ROIC':
            st.subheader('ROIC')
            st.write(f'First {ativos_na_carteira} stocks')
            sorted_df = filtered_df.sort_values(by=['ROIC', 'Liq.2meses'], ascending=[False, False])
            sorted_df['Quantity'] = round((valor_total/ativos_na_carteira)/sorted_df['Price'], 0)
            sorted_df['Amount'] = sorted_df['Quantity'] * sorted_df['Price']
            sorted_df = sorted_df[['Price','ROIC', 'Liq.2meses', 'Quantity', 'Amount']].head(ativos_na_carteira)
            st.table(sorted_df.style.format(precision=2))

    # Referências
    st.sidebar.markdown('---')
    st.sidebar.subheader('References:')
    st.sidebar.write('ROMAN, Gabriel. Avaliação da Eficiência da Magic Formula e de Estratégias de Value Investing para o Mercado Brasileiro. Porto Alegre: UFRGS, 2021.')
    st.sidebar.download_button(label="Download PDF", data='references/TCC Magic Formula.pdf', file_name="TCC_Magic_Formula.pdf", mime="application/pdf")
    st.sidebar.write('ZEIDLER, Rodolfo Gunther Dias. Eficiência da Magic Formula de Value Investing no Mercado Brasileiro. São Paulo: FGV, 2014.')
    st.sidebar.download_button(label="Download PDF", data='references/FGV Magic Formula.pdf', file_name="FGV_Magic_Formula.pdf", mime="application/pdf")



            



            