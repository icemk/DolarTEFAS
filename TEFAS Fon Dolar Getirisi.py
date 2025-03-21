import streamlit as st
import pandas as pd
import plotly.express as px
import yfinance as yf
from datetime import datetime, timedelta

# If you use tefas.fetch, import the library or define it.
# from tefas import Crawler

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
    # If using tefas.fetch:
    # tefas = Crawler()  # or your own instantiation
    # We'll simulate with a placeholder that returns empty DataFrame
    # Replace with your real logic.

    all_data = []
    for i in range(0, len(date_list) - 1, 2):
        start = date_list[i]
        end = date_list[i + 1]
        st.write(f"Fetching data from {start} to {end} for fund {fund_code}...")

        # Example real code:
        # data = tefas.fetch(
        #     start=start,
        #     end=end,
        #     name=fund_code,
        #     columns=["code", "date", "price"]
        # )

        # Placeholder to demonstrate structure
        data = pd.DataFrame({
            "code": [fund_code],
            "date": [start],
            "price": [100.0],
        })
        all_data.append(data)

    if all_data:
        final_data = pd.concat(all_data, ignore_index=True)
    else:
        final_data = pd.DataFrame(columns=["code", "date", "price"])

    final_data["date"] = pd.to_datetime(final_data["date"])
    final_data.sort_values(by="date", inplace=True, ignore_index=True)
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
    usdtry_close_df = usdtry_data[["Close"]].reset_index()

    # Rename columns for merging convenience
    usdtry_close_df.rename(columns={"Date": "date", "Close": "USDTRY_Close"}, inplace=True)

    # Convert date to naive datetime (no tz) so it matches final_data
    usdtry_close_df["date"] = pd.to_datetime(usdtry_close_df["date"]).dt.tz_localize(None)

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
        on="date",
        how="left"
    )
    return final_data_with_fx


###############################################################################
# 5) Compute USD Price and Relative Return
###############################################################################
def compute_usd_price_and_return(final_data_with_fx):
    """
    1) USD_price = price / USDTRY_Close
    2) last_valid_usd_price is the last non-zero USD_price
    3) return_to_last = (last_valid_usd_price / USD_price) - 1
    """
    # Compute USD price
    final_data_with_fx["USD_price"] = (
        final_data_with_fx["price"] / final_data_with_fx["USDTRY_Close"]
    )

    # Find the last valid (non-zero, non-NaN) USD_price
    valid_prices = final_data_with_fx.loc[
        (final_data_with_fx["USD_price"].notna()) & (final_data_with_fx["USD_price"] > 0),
        "USD_price"
    ]

    if len(valid_prices) == 0:
        final_data_with_fx["return_to_last"] = None
        return final_data_with_fx

    last_valid_usd_price = valid_prices.iloc[-1]

    final_data_with_fx["return_to_last"] = (
        last_valid_usd_price / final_data_with_fx["USD_price"] - 1
    )

    # Round for clarity
    final_data_with_fx["USD_price"] = final_data_with_fx["USD_price"].round(4)
    final_data_with_fx["return_to_last"] = final_data_with_fx["return_to_last"].round(4)

    return final_data_with_fx


###############################################################################
# 6) Plot with Plotly (Interactive Hover)
###############################################################################
def plot_return_bar(final_data_with_fx):
    """
    Plot an interactive bar chart of 'return_to_last' vs. 'date' in Plotly.
    Hover shows date and return in percentage.
    """
    if "code" in final_data_with_fx.columns and not final_data_with_fx.empty:
        fund_code = final_data_with_fx["code"].iloc[0]
    else:
        fund_code = "Unknown Fund"

    fig = px.bar(
        final_data_with_fx,
        x="date",
        y="return_to_last",
        labels={
            "date": "Tarih",
            "return_to_last": "Dolar Getirisi (%)"
        },
        title=f"{fund_code} Fonunun Alım Tarihlerine Göre Bugünkü Dolar Bazlı Getirisi",
        hover_data={"return_to_last": ':.2%'}
    )

    fig.update_layout(
        yaxis_tickformat=".0%",
        xaxis_tickangle=-45
    )
    return fig


###############################################################################
# 7) Streamlit App
###############################################################################
def main():
    st.title("TEFAS Fund USD-Based Return Calculator")

    # Let user enter a TEFAS fund code
    fund_code = st.text_input("Enter the TEFAS fund code:", value="YAC")

    if st.button("Run Analysis"):
        with st.spinner("Generating date list..."):
            date_list = generate_date_list()

        with st.spinner("Fetching TEFAS data..."):
            final_data = fetch_tefas_data(date_list, fund_code)

        st.write("**TEFAS Raw Data**", final_data)

        with st.spinner("Fetching USD/TRY exchange rates..."):
            usdtry_close_df = fetch_usdtry_data(date_list)

        st.write("**USD/TRY Raw Data**", usdtry_close_df)

        with st.spinner("Merging data..."):
            final_data_with_fx = merge_data(final_data, usdtry_close_df)
            final_data_with_fx = compute_usd_price_and_return(final_data_with_fx)

        st.write("**Merged Data with USD Price & Returns**", final_data_with_fx)

        # Plot the bar chart
        fig = plot_return_bar(final_data_with_fx)
        st.plotly_chart(fig)

        st.success("Done!")


if __name__ == "__main__":
    main()
