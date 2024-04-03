import fundamentus as fd
import pandas as pd
import numpy as np
import streamlit as st


def credits():
    st.sidebar.header("Sobre o Autor")
    st.sidebar.write('Este aplicativo foi criado por Gustavo Correa usando Streamlit.')
    st.sidebar.markdown('Para mais informações, visite [meu github](https://github.com/gusascorrea) \
                        ou [meu linkedin](https://linkedin.com/in/gustavo-correa--).')
    st.sidebar.markdown('---')

def credits_eng():
    st.sidebar.header("About the Author")
    st.sidebar.write('This application was created by XXXXXXX Correa using Streamlit.')
    st.sidebar.markdown('For more information, visit [my github](https://github.com/gusascorrea) \
                        or [my linkedin](https://linkedin.com/in/gustavo-correa--).')
    st.sidebar.markdown('---')

def homepage():
    st.header('Início')
    st.markdown('---')
    st.header('Objetivo')
    st.write('Esta aplicação foi desenvolvida com o intuito de informar, capacitar e facilitar \
                o usuário a selecionar ações de forma eficiente e eficaz. A técnica abordada torna objetivo \
                o processo de decisão de compra e venda, com risco abaixo do risco médio do mercado (em nosso caso, o Ibovespa) e retornos \
                consistentemente superiores.')
    st.write('**Aviso Legal: esta aplicação não possui caráter de recomendação de investimento, somente informativo.\
              A decisão de investimento deve ser tomada individualmente pelo usuário que utiliza dessas informações.**')

    st.markdown('---')
    st.header('Estratégia')
    st.write('A estratégia é baseada na geração de um ranking de ações que possuem seu valor de mercado \
             descontado com relação ao seu resultado operacional. Existem diversas métricas que nos fornecem essa informação.\
             As métricas utilizadas neste trabalho são:')
    st.write('1. Earnings Yield: Razão entre o lucro operacional (aqui representado pelo EBIT) e o valor de mercado da empresa (Enterprise Value).')
    st.write('2. ROIC: Retorno sobre o capital investido.')
    st.write('3. Magic Formula: Estratégia que combina o ranking Earnings Yield com o ranking ROIC.')
    st.write('Com os métodos de ranking explicitados, compra-se em mesma proporção um conjunto de **N** ações que se encontram\
              no topo desse ranking e que possuam Margem EBIT positiva e uma liquidez mínima.') 
    st.write('A cada período de balanceamento, vende-se as ações que saíram da lista e compra-se as que entraram com o valor de venda.')
    st.write('Os períodos de balanceamentos mais comuns são o mensal, trimestral (na frequência de divulgação dos balanços)\
              e anual (na frequência de divulgação dos balanços mais completos).')
    
def homepage_eng():
    st.header('Home')
    st.markdown('---')
    st.header('Goal')
    st.write('This application was developed with the intention of informing, \
                enabling and facilitating the user to select stocks in a efficient and effective way. \
                The technique used in this application is to generate a ranking of stocks that have their market value \
                discounted with respect to their operational results. There are several metrics that provide this information. \
                The metrics used in this work are:')
    st.write('1. Earnings Yield: The ratio between the operational profit (here represented by EBIT) and the market value of the company (Enterprise Value).')
    st.write('2. ROIC: Return on Invested Capital.')
    st.write('3. Magic Formula: Strategy that combines the ranking Earnings Yield with the ranking ROIC.')
    st.write('With the ranking methods explicitly stated, buy-in in the same proportion a set of **N** stocks that are at the top of this ranking \
              and that have a positive Margem EBIT and a minimum liquidity.')

def study():
    st.header('Estudo')
    st.markdown('---')
    st.write('Este trabalho possui base em um estudo realizado no trabalho de \
             conclusão do Curso de Graduação em Ciências Econômicas na Universidade Federal do Rio Grande do Sul\
             do Economista Gabriel Roman.')

    st.write('O trabalho de referência possui também estratégias de dois fatores que não foram contempladas\
             neste trabalho pelo fato de atualmente não existirem fontes estáveis, confiáveis e gratuitas de dados de\
             cotações em tempo real. Quando houverem melhores soluções, contemplaremos as demais estratégias abordadas\
             no trabalho de conclusão de curso.')

    st.markdown('---')
    st.header('Resultados')

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

def study_eng():
    st.header('Study')
    st.markdown('---')
    st.write('This work was based on a real study done in the work of \
             graduation in Economics at the Federal University of Rio Grande do Sul \
             of Economist Gabriel Roman.')

    st.write('The reference work also has strategies of two factors that were not considered\
             in this work because currently there are no reliable, free and gratuitable data sources\
             of real-time quotes. When there are better solutions, we will consider the other strategies\
             discussed in the work of graduation.')

    st.markdown('---')
    st.header('Results')

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

    st.write('CAGR: cumulative annual return')
    st.write('Sharpe Index: compares the portfolio risk/return with the risk free rate (SELIC)')
    st.write('Rolling-Year-Win: aims to compare the performance of the strategy with the market in moving windows and identify the percentage of time in which the portfolios performed above the market in the same periods')
    st.write('Retorno acumulado para R$100, 00 da Estratégia Earnings Yield (2000 - 2020)')
    st.image('images/ey_ibov.PNG')
    col1, col2, col3 = st.columns(3)
    with col2:
        st.write('Source: ROMAN, Gabriel. 2021.')


def stock_list():
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

        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc['Total'] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc['Total', ['Cotação','Earnings Yield', 'Liq.2meses', 'Quantidade']] = '-'

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
        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc['Total'] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc['Total', ['Cotação','Earnings Yield','ROIC', 'Magic Formula', 'Liq.2meses', 'Quantidade']] = '-'

    elif estrategia == 'ROIC':
        st.subheader('ROIC')
        st.write(f'Primeiros {ativos_na_carteira} ativos')
        sorted_df = filtered_df.sort_values(by=['ROIC', 'Liq.2meses'], ascending=[False, False])

        sorted_df['Quantidade'] = round((valor_total/ativos_na_carteira)/sorted_df['Cotação'], 0)

        sorted_df['Valor'] = sorted_df['Quantidade'] * sorted_df['Cotação']

        sorted_df = sorted_df[['Cotação','ROIC', 'Liq.2meses', 'Quantidade', 'Valor']].head(ativos_na_carteira)

        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc['Total'] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc['Total', ['Cotação','ROIC', 'Liq.2meses', 'Quantidade']] = '-'


    st.table(sorted_df.style.format(precision=2))

def stock_list_eng():
    st.header('Stock List')
    st.markdown('---')

    st.subheader('Strategy')

    estrategia = st.selectbox('Select the strategy', ['Earnings Yield', 'Magic Formula', 'ROIC'])

    ativos_na_carteira = st.number_input('Enter the number of assets in your portfolio:', value = 20)

    st.markdown('---')

    st.subheader('Investment')

    valor_total = st.number_input('Enter the amount you want to invest in the strategy:', value = 0)

    st.markdown('---')

    st.subheader('Liquidity Filter')

    vol_min = st.number_input('Enter the minimum financial volume of the last 2 months:', value = 400000)

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
    filtered_df['Earnings Yield'] = round(1/filtered_df['EV/EBIT'] * 100, 1)
    filtered_df['ROIC'] = round(filtered_df['ROIC'] * 100, 1)

    if estrategia == 'Earnings Yield':
        st.subheader('Earnings Yield')
        st.write(f'First {ativos_na_carteira} assets')
        sorted_df = filtered_df.sort_values(by=['EV/EBIT', 'Liq.2meses'], ascending=[True, False])

        sorted_df['Quantidade'] = round((valor_total/ativos_na_carteira)/sorted_df['Cotação'], 0)

        sorted_df['Valor'] = sorted_df['Quantidade'] * sorted_df['Cotação']

        sorted_df = sorted_df[['Cotação','Earnings Yield', 'Liq.2meses', 'Quantidade', 'Valor']].head(ativos_na_carteira)

        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc['Total'] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc['Total', ['Cotação','Earnings Yield', 'Liq.2meses', 'Quantidade']] = '-'

    elif estrategia == 'Magic Formula':
        st.subheader('Magic Formula')
        st.write(f'First {ativos_na_carteira} assets')
        filtered_df['Ranking_Earning_Yield'] = filtered_df['Earnings Yield'].rank(ascending=False)
        filtered_df['Ranking_ROIC'] = filtered_df['ROIC'].rank(ascending=False)
        filtered_df['Magic Formula'] = filtered_df['Ranking_Earning_Yield'] + filtered_df['Ranking_ROIC']

        sorted_df = filtered_df.sort_values(by=['Magic Formula', 'Liq.2meses'], ascending=[True, False])
        sorted_df['Quantidade'] = round((valor_total/ativos_na_carteira)/sorted_df['Cotação'], 0)

        sorted_df['Valor'] = sorted_df['Quantidade'] * sorted_df['Cotação']

        sorted_df = sorted_df[['Cotação','Earnings Yield','ROIC', 'Magic Formula', 'Liq.2meses', 'Quantidade', 'Valor']].head(ativos_na_carteira)
        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc['Total'] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc['Total', ['Cotação','Earnings Yield','ROIC', 'Magic Formula', 'Liq.2meses', 'Quantidade']] = '-'

    elif estrategia == 'ROIC':
        st.subheader('ROIC')
        st.write(f'First {ativos_na_carteira} assets')
        sorted_df = filtered_df.sort_values(by=['ROIC', 'Liq.2meses'], ascending=[False, False])

        sorted_df['Quantidade'] = round((valor_total/ativos_na_carteira)/sorted_df['Cotação'], 0)

        sorted_df['Valor'] = sorted_df['Quantidade'] * sorted_df['Cotação']

        sorted_df = sorted_df[['Cotação','ROIC', 'Liq.2meses', 'Quantidade', 'Valor']].head(ativos_na_carteira)

        # adiciona linha com soma dos valores apenas na coluna 'Valor'
        sorted_df.loc['Total'] = sorted_df.sum()
        # fora da coluna valor, Total retorna '-'
        sorted_df.loc['Total', ['Cotação','ROIC', 'Liq.2meses', 'Quantidade']] = '-'

    st.table(sorted_df.style.format(precision=2))

def rebalancing():
    st.header('Rebalanceamento')
    st.markdown('---')
    st.write('Escrever Rebalanceamento')

def references():
    st.sidebar.markdown('---')
    st.sidebar.subheader('Referências:')
    st.sidebar.write('ROMAN, Gabriel. Avaliação da Eficiência da Magic Formula e de Estratégias de Value Investing para o Mercado Brasileiro. Porto Alegre: UFRGS, 2021.')
    st.sidebar.download_button(label="Download PDF", data='references/TCC Magic Formula.pdf', file_name="TCC_Magic_Formula.pdf", mime="application/pdf")
    st.sidebar.markdown('Magic Formula para o mercado americano: [Site Magic Formula](https://www.magicformulainvesting.com/)')
    st.sidebar.markdown('Fonte de dados: [API do site Fundamentus](https://pypi.org/project/fundamentus/)')

def references_eng():
    st.sidebar.markdown('---')
    st.sidebar.subheader('References:')
    st.sidebar.write('ROMAN, Gabriel. Avaliação da Eficiência da Magic Formula e de Estratégias de Value Investing para o Mercado Brasileiro. Porto Alegre: UFRGS, 2021.')
    st.sidebar.download_button(label="Download PDF", data='references/TCC Magic Formula.pdf', file_name="TCC_Magic_Formula.pdf", mime="application/pdf")
    st.sidebar.markdown('Magic Formula for the US market: [Site Magic Formula](https://www.magicformulainvesting.com/)')
    st.sidebar.markdown('Source of data: [API of Fundamentus website](https://pypi.org/project/fundamentus/)')

def main():

    # Lançado oficialmente 03/04/2024
    #language = st.sidebar.radio("Select Language", ['Português', 'English'])
    language = 'Português'

    if language == 'Português':
        credits()
        st.title('Value Investing')
        st.markdown('---')

        painel = st.sidebar.radio('Painel', ['Início','Estudo', 'Lista de Ações'])

        if painel == 'Início':
            homepage()
        elif painel == 'Estudo':
            study()
        elif painel == 'Lista de Ações':
            stock_list()
        elif painel == 'Rebalanceamento':
            rebalancing()

        references()
    elif language == 'English':
        credits_eng()
        st.title('Value Investing')
        st.markdown('---')

        painel = st.sidebar.radio('Panel', ['Home', 'Study', 'Stock List'])

        if painel == 'Home':
            homepage_eng()
        elif painel == 'Study':
            study_eng()
        elif painel == 'Stock List':
            stock_list_eng()
        elif painel == 'Rebalanceamento':
            rebalancing_eng()

        references_eng()

if __name__ == "__main__":
    main()




            



            
