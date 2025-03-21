import streamlit as st
import pandas as pd
import plotly.express as px
import yfinance as yf
from datetime import datetime, timedelta
from tefas import Crawler

###############################################################################
# 1) Generate Custom Date List
###############################################################################
def generate_date_list():
    """
    Generate a list of dates starting from 2024-01-01 up to today in 60-day
    increments, skipping the consecutive day for the very first date, but adding
    consecutive days for all subsequent intervals. Also includes today's date.
    """
    start_date = datetime.strptime("2024-01-01", "%Y-%m-%d")
    end_date = datetime.today()

    date_list = []
    current_date = start_date
    prev_date = None  # will store the previous 60-day date

    while current_date < end_date:
        date_list.append(current_date.strftime("%Y-%m-%d"))

        # Skip adding the consecutive day for the *very first* date
        if prev_date and prev_date != start_date:
            consecutive_day = (prev_date + timedelta(days=1)).strftime("%Y-%m-%d")
            date_list.append(consecutive_day)

        prev_date = current_date
        current_date += timedelta(days=60)

    # After the loop ends, add a +1 day after the *last* interval (if not first)
    if prev_date and prev_date != start_date:
        final_consecutive_day = (prev_date + timedelta(days=1)).strftime("%Y-%m-%d")
        date_list.append(final_consecutive_day)

    # Finally, append today's date
    date_list.append(datetime.today().strftime("%Y-%m-%d"))

    # Sort for a clean chronological order (and remove duplicates, if any)
    date_list = sorted(set(date_list))
    return date_list


###############################################################################
# 2) Fetch TEFAS Data in Pairs
###############################################################################
def fetch_tefas_data(date_list, fund_code):
    """
    For every *pair* of dates in date_list, fetch TEFAS data for [start, end).
    Returns a single DataFrame.
    """
    tefas = Crawler()
    all_data = []
    
    # Step through date_list in steps of 2: (0->1, 2->3, etc.)
    for i in range(0, len(date_list) - 1, 2):
        start = date_list[i]
        end = date_list[i + 1]

        data = tefas.fetch(
            start=start, 
            end=end, 
            name=fund_code, 
            columns=["code", "date", "price"]
        )
        all_data.append(data)

    # Combine all segments into a single DataFrame
    final_data = pd.concat(all_data, ignore_index=True)

    # Convert date to datetime for proper sorting/merging
    final_data['date'] = pd.to_datetime(final_data['date'])
    final_data.sort_values(by='date', inplace=True, ignore_index=True)
    return final_data


###############################################################################
# 3) Fetch USD/TRY Data from Yahoo Finance
###############################################################################
def fetch_usdtry_data(date_list):
    """
    Fetch daily USD/TRY rates (Close) from Yahoo Finance for the entire range
    from date_list[0] to date_list[-1] (inclusive).
    Returns a DataFrame with columns ['date', 'USDTRY_Close'].
    """
    ticker = "USDTRY=X"
    start_dt = datetime.strptime(date_list[0], "%Y-%m-%d")
    end_dt = datetime.strptime(date_list[-1], "%Y-%m-%d")

    # Add +1 day because yfinance excludes the end date
    end_dt += timedelta(days=1)

    usdtry = yf.Ticker(ticker)
    usdtry_data = usdtry.history(
        start=start_dt.strftime("%Y-%m-%d"),
        end=end_dt.strftime("%Y-%m-%d")
    )

    # Keep only the 'Close' column
    usdtry_close_df = usdtry_data[['Close']].reset_index()

    # Rename columns for merging convenience
    usdtry_close_df.rename(columns={'Date': 'date', 'Close': 'USDTRY_Close'}, inplace=True)
    
    # Convert date to datetime
    usdtry_close_df['date'] = pd.to_datetime(usdtry_close_df['date'])
    # Remove any timezone information
    usdtry_close_df['date'] = usdtry_close_df['date'].dt.tz_localize(None)
    
    return usdtry_close_df


###############################################################################
# 4) Merge TEFAS Data with USDTRY Data
###############################################################################
def merge_data(final_data, usdtry_close_df):
    """
    Left-merge final_data (TEFAS) with usdtry_close_df (USDTRY rates) on 'date'.
    Returns a combined DataFrame.
    """
    final_data_with_fx = pd.merge(
        final_data,
        usdtry_close_df,
        on='date',
        how='left'
    )
    return final_data_with_fx


###############################################################################
# 5) Compute USD Price and Relative Return
###############################################################################
def compute_usd_price_and_return(final_data_with_fx):
    """
    1) USD_price = price / USDTRY_Close
    2) last_valid_usd_price is the last non-zero USD_price
    3) getiri = (last_valid_usd_price / USD_price) - 1
    """
    # Compute USD price
    final_data_with_fx['USD_price'] = (
        final_data_with_fx['price'] / final_data_with_fx['USDTRY_Close']
    )

    # Find the last valid (non-zero, non-NaN) USD_price
    valid_prices = final_data_with_fx.loc[
        (final_data_with_fx['USD_price'].notna()) & (final_data_with_fx['USD_price'] > 0),
        'USD_price'
    ]
    
    if len(valid_prices) == 0:
        # Edge case: no valid USD_price
        final_data_with_fx['getiri'] = None
        return final_data_with_fx

    last_valid_usd_price = valid_prices.iloc[-1]

    # Compute relative return vs. last valid price
    final_data_with_fx['getiri'] = (
        last_valid_usd_price / final_data_with_fx['USD_price'] - 1
    )

    # Round for clarity
    final_data_with_fx['USD_price'] = final_data_with_fx['USD_price'].round(4)
    final_data_with_fx['getiri'] = final_data_with_fx['getiri'].round(4)
    final_data_with_fx['date'] = final_data_with_fx['date'].dt.strftime('%Y-%m-%d')

    # Convert to percentage string with 1 decimal place, then add '%'
    final_data_with_fx['getiri'] = (final_data_with_fx['getiri'].apply(lambda x: f"{x*100:.1f}%" if pd.notnull(x) else ""))
    return final_data_with_fx


###############################################################################
# 6) Plot with Plotly (Interactive Hover)
###############################################################################
def plot_return_bar(final_data_with_fx):
    fund_code = final_data_with_fx['code'].iloc[0] if 'code' in final_data_with_fx.columns else 'Unknown Fund'

    fig = px.bar(
        final_data_with_fx,
        x='date',
        y='getiri',
        labels={
            'date': 'Tarih',
            'getiri': 'Dolar Getirisi (%)'
        },
        title=f'{fund_code} Fonunun Alım Tarihlerine Göre Bugünkü Dolar Bazlı Getirisi',
        # Show numeric with 1 decimal on hover (e.g., "12.3")
        hover_data={'getiri': ':.1f'}
    )

    # Add a % suffix on the axis
    fig.update_layout(
        xaxis_tickangle=-45,
        yaxis=dict(
            tickformat=".1f",
            ticksuffix="%"
        )
    )

    return fig


########################################################################
# ANNUALIZED PORTION
########################################################################
def compute_annualized_return_percent(df_fund):
    """
    Compute annualized return for each row based on:
        annualized_return = [(1 + TR_fraction)^(365 / holding_days)] - 1
    where:
      - 'total_return_percent' is the total return in percent, e.g. 12.3 for +12.3%
      - 'date' is the date of purchase
      - holding_days = (final_date - current_row_date).days
    The result is stored in 'annualized_return_percent' as a percentage, e.g. 15.0 for +15.0%.
    """
    if 'total_return_percent' not in df_fund.columns:
        raise ValueError("'total_return_percent' column is missing. Please rename or create it first.")

    # Final date is the *last* date in df_fund
    final_date = df_fund['date'].iloc[-1]

    # Number of days from each row’s date to the final date
    df_fund['holding_days'] = (final_date - df_fund['date']).dt.days

    # Avoid division by zero if the row is the final_date
    df_fund.loc[df_fund['holding_days'] == 0, 'holding_days'] = 1

    # Convert the total_return_percent to a fraction, e.g. 12.3 => 0.123
    total_return_fraction = df_fund['total_return_percent'] / 100.0

    # Compute annualized return in fraction form
    annualized_return_fraction = (1 + total_return_fraction) ** (365 / df_fund['holding_days']) - 1

    # Convert fraction back to a percentage, e.g. 0.15 => 15.0
    df_fund['annualized_return_percent'] = annualized_return_fraction * 100

    return df_fund


def plot_annualized_return_bar(df_fund):
    """
    Plot an interactive bar chart of 'annualized_return_percent' vs. 'date'.
    - 'annualized_return_percent': e.g. 15.2 => +15.2% annualized
    """
    fund_code = df_fund['code'].iloc[0] if 'code' in df_fund.columns else 'Unknown Fund'

    fig = px.bar(
        df_fund,
        x='date',
        y='annualized_return_percent',
        labels={
            'date': 'Tarih',
            'annualized_return_percent': 'Yıllıklandırılmış Getiri (%)'
        },
        title=f'{fund_code} Fonunun Alım Tarihlerine Göre Yıllıklandırılmış Getiri (Yüzde)',
        hover_data={'annualized_return_percent': ':.1f'}  # show 1 decimal place
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        yaxis=dict(
            tickformat=".1f",  # numeric + 1 decimal
            ticksuffix="%"     # "12.3%"
        )
    )
    return fig



###############################################################################
# 7) Streamlit App
###############################################################################
def run_workflow(fund_code):
    # Generate date_list
    date_list = generate_date_list()

    # Fetch TEFAS data
    final_data = fetch_tefas_data(date_list, fund_code=fund_code)

    # Fetch USD/TRY data
    usdtry_close_df = fetch_usdtry_data(date_list)

    # Merge
    final_data_with_fx = merge_data(final_data, usdtry_close_df)

    # Compute USD price and return
    final_data_with_fx = compute_usd_price_and_return(final_data_with_fx)

    # Plot bar chart with Plotly
    fig = plot_return_bar(final_data_with_fx)

    return final_data_with_fx, fig


def main():
    st.set_page_config(layout="wide")
    st.title("TEFAS - Dolar Getirisi Hesaplama")
    st.write(
    """
    Bu uygulama sayesinde fon yatırımlarınızın dolar bazlı getirisini öğrenebilirsiniz!
    """
    )

    # User input for the fund code
    fund_code = st.text_input("TEFAS fonu gir (örneğin 'BGP'):", value="BGP")

    if st.button("Run"):
        if not fund_code.strip():
            st.warning("Lütfen fon kodu girin.")
        else:
            with st.spinner("Hesaplanıyor..."):
                try:
                    final_data_with_fx, fig = run_workflow(fund_code)
                    st.success("Başarıyla tamamlandı!")
                    st.plotly_chart(fig, use_container_width=True)
                    
                    # Optionally show the merged DataFrame
                    st.write("İlgilenenler için grafik verisi:")
                    st.dataframe(final_data_with_fx)

                    df_fund = compute_annualized_return_percent(df_fund)

                    # 3) Show the annualized return chart
                    fig_annual = plot_annualized_return_bar(df_fund)
                    st.plotly_chart(fig_annual, use_container_width=True)
                
                    # Optionally show the DataFrame
                    st.write("Final DataFrame columns:")
                    st.dataframe(df_fund)
                except Exception as e:
                    st.error(f"Hata: {e}")
    

if __name__ == "__main__":
    main()
