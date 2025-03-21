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
def compute_usd_price_and_return(df):
    """
    1) USD_price = price / USDTRY_Close
    2) last_valid_usd_price is the last valid USD_price
    3) fractional_return = (last_valid_usd_price / USD_price) - 1
    4) total_return_percent = fractional_return * 100
    """
    df['USD_price'] = df['price'] / df['USDTRY_Close']

    # Find the last valid (non-zero, non-NaN) USD_price
    valid_prices = df.loc[
        df['USD_price'].notna() & (df['USD_price'] > 0),
        'USD_price'
    ]
    if valid_prices.empty:
        df['total_return_percent'] = None
        return df

    last_valid_usd_price = valid_prices.iloc[-1]

    # Compute fractional return
    fractional_return = last_valid_usd_price / df['USD_price'] - 1

    # Convert fraction -> percentage (e.g. 0.12 -> 12.0)
    df['total_return_percent'] = fractional_return.mul(100).round(1)

    # Keep 'date' as datetime for potential future calculations
    # Create a separate string column for display
    df['date'] = df['date'].dt.strftime('%Y-%m-%d')

    return df



###############################################################################
# 6) PLOT: Total Return Bar
###############################################################################
def plot_return_bar(df):
    """
    Plots 'total_return_percent' vs. 'date_str'.
    - total_return_percent is numeric (e.g. 12.3 means +12.3%)
    """
    fund_code = df['code'].iloc[0] if 'code' in df.columns else 'Unknown Fund'

    fig = px.bar(
        df,
        x='date_str',               # use the string version on x-axis
        y='total_return_percent',   # numeric percentage
        labels={
            'date_str': 'Tarih',
            'total_return_percent': 'Dolar Getirisi (%)'
        },
        title=f'{fund_code} Fonunun Alım Tarihlerine Göre Bugünkü Dolar Bazlı Getirisi',
        hover_data={'total_return_percent': ':.1f'}  # e.g. 12.3
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        yaxis=dict(
            tickformat=".1f",   # e.g. "12.3"
            ticksuffix="%"      # => "12.3%"
        )
    )

    return fig


########################################################################
# ANNUALIZED PORTION
########################################################################
def compute_annualized_return_percent(df):
    """
    Compute annualized return for each row based on:
        annualized_fraction = [(1 + (total_return_percent/100))^(365 / holding_days)] - 1
    BUT only if holding_days >= 30.
    Otherwise, set annualized_return_percent to NaN.
    
    'total_return_percent' is a numeric percentage column, e.g. 12.3 => +12.3%.
    """
    if 'total_return_percent' not in df.columns:
        raise ValueError("'total_return_percent' column is missing.")

    # final_date is the last date in df
    final_date = df['date'].iloc[-1]

    # holding_days = # of days from each row's date to final_date
    df['holding_days'] = (final_date - df['date']).dt.days

    # Avoid division by zero if the row is the final_date
    df.loc[df['holding_days'] == 0, 'holding_days'] = 1

    # Make a mask for rows with holding_days >= 30
    mask_30plus = df['holding_days'] >= 30

    # Convert total_return_percent to fraction (e.g. 12.3 => 0.123)
    frac = df['total_return_percent'] / 100.0

    # Calculate annualized return (in fraction form) only for rows with >= 30 days
    # For others, we'll set it to NaN
    annualized_fraction = (1 + frac) ** (365 / df['holding_days']) - 1

    # Create a new column, start with all NaN
    df['annualized_return_percent'] = float('nan')

    # Fill in annualized returns only where holding_days >= 30
    df.loc[mask_30plus, 'annualized_return_percent'] = (
        annualized_fraction[mask_30plus].mul(100).round(1)
    )
    df['date'] = df['date'].dt.date

    return df



def plot_annualized_return_bar(df):
    """
    Plot an interactive bar chart of 'annualized_return_percent' vs. 'date_str'.
    - 15.2 => +15.2% annualized
    """
    fund_code = df['code'].iloc[0] if 'code' in df.columns else 'Unknown Fund'

    fig = px.bar(
        df,
        x='date_str',
        y='annualized_return_percent',
        labels={
            'date_str': 'Tarih',
            'annualized_return_percent': 'Yıllıklandırılmış Getiri (%)'
        },
        title=f'{fund_code} Fonunun Alım Tarihlerine Göre Yıllıklandırılmış Getirisi',
        hover_data={'annualized_return_percent': ':.1f'}
    )

    fig.update_layout(
        xaxis_tickangle=-45,
        yaxis=dict(
            tickformat=".1f",
            ticksuffix="%"
        )
    )
    return fig


###############################################################################
# 7) Streamlit App
###############################################################################
def run_workflow(fund_code):
    # 1) Generate date_list
    date_list = generate_date_list()

    # 2) Fetch TEFAS data
    df_tefas = fetch_tefas_data(date_list, fund_code=fund_code)

    # 3) Fetch USD/TRY data
    df_usdtry = fetch_usdtry_data(date_list)

    # 4) Merge
    df_merged = merge_data(df_tefas, df_usdtry)

    # 5) Compute USD price & total return in percent
    df_with_return = compute_usd_price_and_return(df_merged)

    # 6) Plot bar chart with Plotly
    fig_total = plot_return_bar(df_with_return)

    return df_with_return, fig_total


def main():
    st.set_page_config(layout="wide")
    st.title("TEFAS - Dolar Getirisi Hesaplama")
    st.write(
        """
        Bu uygulama sayesinde fon yatırımlarınızın dolar bazlı getirisini
        ve yıllıklandırılmış getiriyi hesaplayabilirsiniz!
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
                    # Get the main DataFrame + total return chart
                    df_fund, fig_total = run_workflow(fund_code)

                    st.success("Başarıyla tamamlandı!")
                    st.plotly_chart(fig_total, use_container_width=True)

                    # Display the DataFrame with total_return_percent
                    st.write("Detaylı veri tablosu:")
                    st.dataframe(df_fund)

                    # --- Compute & Plot Annualized Return ---
                    df_fund2 = compute_annualized_return_percent(df_fund)
                    fig_annual = plot_annualized_return_bar(df_fund)
                    st.plotly_chart(fig_annual, use_container_width=True)

                    st.write("Detaylı veri tablosu (yıllıklandırılmış getiri hesaplaması için 30 günlük veri gereklidir):")
                    st.dataframe(df_fund2)

                except Exception as e:
                    st.error(f"Hata: {e}")


if __name__ == "__main__":
    main()
