import fundamentus as fd
import pandas as pd
import numpy as np
#import warnings
#from io import StringIO
import streamlit as st

#language = st.sidebar.radio("Select Language", ['Português', 'English'])

#if language == 'Português':

st.sidebar.markdown('---')
painel = st.sidebar.radio('Painel', ['Início'#,'Estudo'
                                        , 'Lista de Ações'])

st.title('Value Investing')
st.markdown('---')

if painel == 'Início':
    st.header('Objetivo')
    st.markdown('---')
    st.write('Esta aplicação foi desenvolvida com o intuito de informar, capacitar e facilitar \
                o usuário a selecionar ações de forma eficiente e eficaz. A técnica abordada torna objetivo \
                o processo de decisão de compra e venda, com risco abaixo do risco médio do mercado (em nosso caso, o Ibovespa) e retornos \
                consistentemente superiores.')
                
                
elif painel == 'Estudo':
    st.header('Estudo')
    st.markdown('---')
    st.write('A Magic Formula é uma estratégia de seleção de ativos desenvolvida por Joel\
    Greenblatt para selecionar ações com altos ROICs e Earnings Yield, representando\
    qualidade e valor respectivamente, medidos por indicadores contábeis das empresas.')

    st.write('')

    st.write('')

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

    st.write('CAGR: retorno anual composto')
    st.write('Índice de Sharpe: comparação entre o risco/retorno do portfolio com a taxa livre de risco (SELIC)')

    st.write('Rolling-Year-Win: tem como objetivo\
            comparar o desempenho da estratégia com o mercado em janelas móveis e identificar\
            qual o percentual de tempo em que as carteiras performaram acima do mercado nos\
            mesmos períodos')
    
    st.subheader('Retorno acumulado para R$100,00 da Estratégia Earnings Yield (2000 - 2020)')
    st.image('images/ey_ibov.PNG')
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write('Fonte: ROMAN, Gabriel. 2021.')

    st.subheader('Comparação entre balanceamento anual e trimestral para estratégia Earnings Yield')
    st.image('images/balanceamento.PNG')
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

    i = 0
    j = 0
    k = 0

    while True:
        fin_ = fd.list_papel_setor(i)  # finance
        if 'BBAS3' in fin_:
            fin = fin_
            j = 1
        seg_ = fd.list_papel_setor(i)  # finance
        if 'WIZC3' in seg_:
            seg = seg_
            k = 1
        if j == 1 and k == 1:
            financeiras = fin + seg
            break
        i+=1

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

elif painel == 'Referências':

    # Referências
    '''st.sidebar.markdown('---')
    st.sidebar.subheader('Referências:')
    st.sidebar.write('ROMAN, Gabriel. Avaliação da Eficiência da Magic Formula e de Estratégias de Value Investing para o Mercado Brasileiro. Porto Alegre: UFRGS, 2021.')
    st.sidebar.download_button(label="Download PDF", data='references/TCC Magic Formula.pdf', file_name="TCC_Magic_Formula.pdf", mime="application/pdf")
    st.sidebar.write('ZEIDLER, Rodolfo Gunther Dias. Eficiência da Magic Formula de Value Investing no Mercado Brasileiro. São Paulo: FGV, 2014.')
    st.sidebar.download_button(label="Download PDF", data='references/FGV Magic Formula.pdf', file_name="FGV_Magic_Formula.pdf", mime="application/pdf")
    '''



            



            